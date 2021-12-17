package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/influxdata/tdigest"
	"github.com/spf13/pflag"

	_ "net/http/pprof"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
)

const (
	S3Region = "us-east-1"
	S3Bucket = "david.golden"
	S3Prefix = "randomdata"
)

type fileSet struct {
	Size int
}

// Filesets have a label to use for selection and a size for all files in that
// set.  Done as a struct in case I need to add more fields.
var fileSets = map[string]fileSet{
	"K001": {
		Size: KiB,
	},
	"K004": {
		Size: 4 * KiB,
	},
	"K016": {
		Size: 16 * KiB,
	},
	"K064": {
		Size: 64 * KiB,
	},
	"K256": {
		Size: 256 * KiB,
	},
	"M001": {
		Size: MiB,
	},
	"M004": {
		Size: 4 * MiB,
	},
	"M016": {
		Size: 16 * MiB,
	},
	"M032": {
		Size: 32 * MiB,
	},
	"M064": {
		Size: 64 * MiB,
	},
	"M128": {
		Size: 128 * MiB,
	},
	"M256": {
		Size: 256 * MiB,
	},
}

type myConfig struct {
	Count             int
	DownloadSizeBytes int
	EC2Instance       string
	FileSetName       string
	Goroutines        int
}

func parseFlags() *myConfig {
	count := pflag.Uint("count", 1, "number of datapoints to generate")
	instance := pflag.String("instance", "unknown", "EC2 instance type")
	goroutines := pflag.Uint("goroutines", uint(runtime.NumCPU()), "parallel downloads")
	fileSetName := pflag.String("set", "M001", "file set to download")
	downloadSize := pflag.Uint("download", 256, "total size to download in MiB")
	pflag.Parse()

	fileSet, ok := fileSets[*fileSetName]
	if !ok {
		log.Fatalf("unknown file set '%s'", *fileSetName)
	}

	dlSize := int(*downloadSize) * MiB
	if dlSize%fileSet.Size != 0 {
		log.Fatalf("downloadMB (%d MiB) must be a multiple of the file set size (%d)", *downloadSize, fileSet.Size)
	}

	dlCount := dlSize / fileSet.Size
	if int(*goroutines) > dlCount {
		log.Fatalf("goroutines (%d) is greater than files to download (%d)", *goroutines, dlCount)
	}

	return &myConfig{
		Count:             int(*count),
		DownloadSizeBytes: dlSize,
		EC2Instance:       *instance,
		FileSetName:       *fileSetName,
		Goroutines:        int(*goroutines),
	}
}

type Datapoint struct {
	// Fixed at run time by config
	EC2Instance    string
	FileSizeBytes  int    // for scatter plotting
	FileSizeLabel  string // for data series labeling
	Goroutines     int
	TotalSizeBytes int

	// Calculated during execution
	ElapsedSecs    float64
	P50Latency     float64 // Req to response, without reading full body
	P95Latency     float64
	P99Latency     float64
	ThroughputMiBs float64 // TotalSizeBytes / MiB / ElapsedSecs
}

func configS3(cfg *myConfig) (*s3.Client, error) {
	// customClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
	// 	tr.MaxIdleConnsPerHost = 1024
	// 	tr.IdleConnTimeout = 1 * time.Minute
	// })

	awscfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(S3Region),
		// config.WithHTTPClient(customClient),
	)
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
			o.RateLimiter = &nopRateLimiter{}
			o.MaxAttempts = 10
		})
	})

	return s3Client, nil
}

func listS3Files(cfg *myConfig, s3Client *s3.Client) ([]string, error) {
	files := make([]string, 0, 1024)

	req := &s3.ListObjectsV2Input{
		Bucket: aws.String(S3Bucket),
		Prefix: aws.String(path.Join(S3Prefix, cfg.FileSetName)),
	}

	// listobjects from S3
	p := s3.NewListObjectsV2Paginator(s3Client, req)

	// Iterate through the S3 object pages, printing each object returned.
	var i int
	for p.HasMorePages() {
		i++
		page, err := p.NextPage(context.Background())
		if err != nil {
			log.Fatalf("failed to get page %v, %v", i, err)
		}

		// objects found
		for _, obj := range page.Contents {
			files = append(files, *obj.Key)
		}
	}

	// shuffle result
	rand.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})

	return files, nil
}

func buildDownloadList(cfg *myConfig, s3Client *s3.Client) ([]string, error) {
	// Download file candidates from s3
	fileList, err := listS3Files(cfg, s3Client)
	if err != nil {
		return nil, err
	}
	if len(fileList) == 0 {
		return nil, errors.New("no S3 files found for file set")
	}

	numFilesNeeded := cfg.DownloadSizeBytes / fileSets[cfg.FileSetName].Size
	if numFilesNeeded == 0 {
		log.Fatal("config results in zero files needed for download; WTF")
	}

	files := make([]string, 0, numFilesNeeded)

	for {
		for _, f := range fileList {
			files = append(files, f)
			if len(files) >= numFilesNeeded {
				return files, nil
			}
		}
	}
}

func downloader(s3Client *s3.Client, work chan string, latency chan float64) {
	for f := range work {
		start := time.Now()
		req := &s3.GetObjectInput{
			Bucket: aws.String(S3Bucket),
			Key:    aws.String(f),
		}
		resp, err := s3Client.GetObject(context.Background(), req)
		if err != nil {
			log.Fatalf("error downloading %s: %v", f, err)
		}
		latency <- time.Since(start).Seconds()
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
	}
}

func run(cfg *myConfig) int {

	// Configure S3 client
	s3Client, err := configS3(cfg)
	if err != nil {
		log.Fatalf("error configuring S3: %v", err)
	}

	// Build a list of files from fileset equal to total download size
	downloadList, err := buildDownloadList(cfg, s3Client)
	if err != nil {
		log.Fatalf("error building file list: %v", err)
	}

	// Let channels be buffered by goroutine count, but not ridiculously to
	// avoid blowing up memory
	chanSize := cfg.Goroutines
	if chanSize > 1024 {
		chanSize = 1024
	}

	// Use goroutine to pump file list into a channel
	work := make(chan string, chanSize)
	go func() {
		for _, f := range downloadList {
			work <- f
		}
		close(work)
	}()

	// Collect latencies
	latency := make(chan float64, chanSize)
	latencyDone := make(chan struct{})
	td := tdigest.NewWithCompression(1000)
	go func() {
		for v := range latency {
			td.Add(v, 1)
		}
		close(latencyDone)
	}()

	// Record start time just before goroutines start.
	startTime := time.Now()

	// Start worker goroutines to download files from channel.  Don't want to
	// synchronize their start because we won't do that in practice in ADL.
	var wg sync.WaitGroup
	for i := 0; i < cfg.Goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			downloader(s3Client, work, latency)
		}()
	}

	// Wait for all downloads to finish
	wg.Wait()
	elapsedSec := time.Since(startTime).Seconds()

	// Wait for latency calculations
	close(latency)
	<-latencyDone

	// Emit statistics as JSON (for later mongoimport to graph results)

	datapoint := Datapoint{
		// Defined
		EC2Instance:    cfg.EC2Instance,
		FileSizeBytes:  fileSets[cfg.FileSetName].Size,
		FileSizeLabel:  cfg.FileSetName,
		Goroutines:     cfg.Goroutines,
		TotalSizeBytes: cfg.DownloadSizeBytes,

		// Calculated
		ElapsedSecs:    elapsedSec,
		P50Latency:     td.Quantile(0.50),
		P95Latency:     td.Quantile(0.95),
		P99Latency:     td.Quantile(0.99),
		ThroughputMiBs: float64(cfg.DownloadSizeBytes) / MiB / elapsedSec,
	}

	jb, err := json.Marshal(datapoint)
	if err != nil {
		log.Fatalf("error encoding datapoint to JSON: %v", err)
	}

	fmt.Println(string(jb))

	return 0
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	cfg := parseFlags()
	var ec int
	for i := 0; i < cfg.Count; i++ {
		ec += run(cfg)
	}
	os.Exit(ec)
}

type nopRateLimiter struct{}

func (*nopRateLimiter) GetToken(_ context.Context, _ uint) (releaseToken func() error, err error) {
	return func() error { return nil }, nil
}
func (*nopRateLimiter) AddTokens(_ uint) error { return nil }
