package s3

// https://github.com/aws/aws-sdk-go
// https://docs.aws.amazon.com/sdk-for-go/api/service/s3.html

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffail/tunny"
	"github.com/whosonfirst/go-whosonfirst-crawl"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-pool"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Sync struct {
	Service       *s3.S3
	ACL           string
	Bucket        string
	Prefix        string
	WorkPool      tunny.WorkPool
	Logger        *log.WOFLogger
	Debug         bool
	Dryrun        bool
	Success       int64
	Error         int64
	Skipped       int64
	Scheduled     int64
	Completed     int64
	Retried       int64
	TimeToProcess *time.Duration
	Retries       *pool.LIFOPool
	MaxRetries    float64 // max percentage of errors over scheduled
}

func NewSync(creds *credentials.Credentials, region string, acl string, bucket string, prefix string, procs int, debug bool, logger *log.WOFLogger) *Sync {

	runtime.GOMAXPROCS(procs)

	workpool, _ := tunny.CreatePoolGeneric(procs).Open()
	retries := pool.NewLIFOPool()

	cfg := aws.NewConfig()
	cfg.WithRegion(region)

	if creds != nil {
		cfg.WithCredentials(creds)
	}

	sess := session.New(cfg)

	svc := s3.New(sess)

	ttp := new(time.Duration)

	return &Sync{
		Service:       svc,
		ACL:           acl,
		Bucket:        bucket,
		Prefix:        prefix,
		WorkPool:      *workpool,
		Debug:         debug,
		Dryrun:        false,
		Logger:        logger,
		Scheduled:     0,
		Completed:     0,
		Skipped:       0,
		Error:         0,
		Success:       0,
		Retried:       0,
		TimeToProcess: ttp,
		Retries:       retries,
		MaxRetries:    25.0, // maybe allow this to be user-defined ?
	}
}

func WOFSync(bucket string, prefix string, procs int, debug bool, logger *log.WOFLogger) *Sync {

	return NewSync(nil, "us-east-1", "public-read", bucket, prefix, procs, debug, logger)
}

func (sink *Sync) SyncDirectory(root string) error {

	defer sink.WorkPool.Close()

	t0 := time.Now()

	wg := new(sync.WaitGroup)

	callback := func(source string, info os.FileInfo) error {

		if info.IsDir() {
			return nil
		}

		err := sink.SyncFile(source, root, wg)

		if err != nil {
			sink.Logger.Error("failed to sync %s, because '%s'", source, err)
		}

		return nil
	}

	c := crawl.NewCrawler(root)
	_ = c.Crawl(callback)

	wg.Wait()

	sink.ProcessRetries(root)

	ttp := time.Since(t0)
	sink.TimeToProcess = &ttp

	return nil
}

func (sink *Sync) SyncFiles(files []string, root string) error {

	defer sink.WorkPool.Close()

	t0 := time.Now()

	wg := new(sync.WaitGroup)

	for _, path := range files {

		sink.Logger.Debug("Sync %s", path)
		sink.SyncFile(path, root, wg)
	}

	wg.Wait()

	sink.ProcessRetries(root)

	ttp := time.Since(t0)
	sink.TimeToProcess = &ttp

	return nil
}

func (sink *Sync) SyncFileList(path string, root string) error {

	defer sink.WorkPool.Close()

	t0 := time.Now()

	file, err := os.Open(path)

	if err != nil {
		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	count := 100000
	throttle := make(chan bool, count)

	go func() {
		for i := 0; i < count; i++ {
			throttle <- true
		}
	}()

	wg := new(sync.WaitGroup)

	for scanner.Scan() {

		<-throttle

		path := scanner.Text()
		sink.Logger.Debug("Schedule %s for sync", path)

		wg.Add(1)

		go func(path string, root string, wg *sync.WaitGroup, throttle chan bool) {

			defer func() {
				wg.Done()
				throttle <- true
			}()

			sink.SyncFile(path, root, wg)
			sink.Logger.Debug("Completed sync for %s", path)

		}(path, root, wg, throttle)
	}

	wg.Wait()

	sink.ProcessRetries(root)

	ttp := time.Since(t0)
	sink.TimeToProcess = &ttp

	return nil
}

func (sink *Sync) SyncFile(source string, root string, wg *sync.WaitGroup) error {

	sink.Logger.Debug("Schedule %s for processing", source)
	atomic.AddInt64(&sink.Scheduled, 1)

	_, err := sink.WorkPool.SendWork(func() {

		wg.Add(1)

		defer wg.Done()

		dest := source

		dest = strings.Replace(dest, root, "", -1)

		if sink.Prefix != "" {
			dest = path.Join(sink.Prefix, dest)
		}

		_, err := os.Stat(source)

		if os.IsNotExist(err) {
			sink.Logger.Debug("Source file (%s) does not exist, skipping", source)
			return
		}

		// Note: both HasChanged and SyncFile will ioutil.ReadFile(source)
		// which is a potential waste of time and resource. Or maybe we just
		// don't care? (20150930/thisisaaronland)

		sink.Logger.Debug("Looking for changes to %s (prefix: %s)", dest, sink.Prefix)

		change, ch_err := sink.HasChanged(source, dest)

		if ch_err != nil {

			atomic.AddInt64(&sink.Completed, 1)
			atomic.AddInt64(&sink.Error, 1)
			sink.Logger.Warning("failed to determine whether %s had changed, because '%s'", source, ch_err)

			sink.Retries.Push(&pool.PoolString{String: source})
			return
		}

		if !change {

			atomic.AddInt64(&sink.Completed, 1)
			atomic.AddInt64(&sink.Skipped, 1)
			sink.Logger.Debug("%s has not changed, skipping", source)
			return
		}

		err = sink.DoSyncFile(source, dest)
		atomic.AddInt64(&sink.Completed, 1)

		if err != nil {
			sink.Retries.Push(&pool.PoolString{String: source})
			atomic.AddInt64(&sink.Error, 1)
		} else {
			atomic.AddInt64(&sink.Success, 1)
		}
	})

	if err != nil {
		atomic.AddInt64(&sink.Error, 1)
		sink.Logger.Error("Failed to schedule %s for processing, because %v", source, err)
		return err
	}

	return nil
}

func (sink *Sync) DoSyncFile(source string, dest string) error {

	sink.Logger.Debug("Prepare %s for syncing", source)

	body, err := ioutil.ReadFile(source)

	if err != nil {
		sink.Logger.Error("Failed to read %s, because %v", source, err)
		return err
	}

	sink.Logger.Debug("PUT %s as %s", dest, sink.ACL)

	params := &s3.PutObjectInput{
		Bucket: aws.String(sink.Bucket),
		Key:    aws.String(dest),
		Body:   bytes.NewReader(body),
		ACL:    aws.String(sink.ACL),
	}

	if sink.Dryrun {
		sink.Logger.Info("Running in dryrun mode so we'll just assume that %s was cloned", dest)
		return nil
	}

	_, err = sink.Service.PutObject(params)

	if err != nil {
		sink.Logger.Error("Failed to PUT %s, because '%s'", dest, err)
		return err
	}

	return nil
}

func (sink *Sync) HasChanged(source string, dest string) (ch bool, err error) {

	sink.Logger.Debug("HEAD s3://%s/%s", sink.Bucket, dest)

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#HeadObjectInput
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#HeadObjectOutput

	params := &s3.HeadObjectInput{
		Bucket: aws.String(sink.Bucket),
		Key:    aws.String(dest),
	}

	// sink.Logger.Debug(params.GoString())

	rsp, err := sink.Service.HeadObject(params)

	if err != nil {

		aws_err := err.(awserr.Error)

		if aws_err.Code() == "NotFound" {
			sink.Logger.Info("%s is 404", dest)
			return true, nil
		}

		sink.Logger.Error("Failed to HEAD %s because %s", dest, err)
		return false, err
	}

	// we used to do the following with a helper function in go-whosonfirst-utils
	// but that package has gotten unweildy and out of control - I am thinking about
	// a generic WOF "hashing" package but that started turning in to quicksand so
	// in the interest of just removing go-whosonfirst-utils as a dependency we're
	// going to do it the old-skool way by hand, for now (20170718/thisisaaronland)

	/*

		 local_hash, err := utils.HashFile(source)
		if err != nil {
			sink.Logger.Warning("Failed to hash %s, because %v", source, err)
			return false, err
		}

	*/

	body, err := ioutil.ReadFile(source)

	if err != nil {
		return false, err
	}

	enc := md5.Sum(body)
	local_hash := hex.EncodeToString(enc[:])

	etag := *rsp.ETag
	remote_hash := strings.Replace(etag, "\"", "", -1)

	sink.Logger.Debug("Local hash is %s remote hash is %s", local_hash, remote_hash)

	if local_hash == remote_hash {
		return false, nil
	}

	// Okay so we think that things have changed but let's just check
	// modification times to be extra sure (20151112/thisisaaronland)

	info, err := os.Stat(source)

	if err != nil {
		sink.Logger.Error("Failed to stat %s because %s", source, err)
		return false, err
	}

	mtime_local := info.ModTime()
	mtime_remote := *rsp.LastModified

	// Because who remembers this stuff anyway...
	// func (t Time) Before(u Time) bool
	// Before reports whether the time instant t is before u.

	sink.Logger.Debug("Local %s %s", mtime_local, source)
	sink.Logger.Debug("Remote %s %s", mtime_remote, dest)

	if mtime_local.Before(mtime_remote) {
		sink.Logger.Warning("Remote copy of %s has a more recent modification date (local: %s remote: %s)", source, mtime_local, mtime_remote)
		return false, nil
	}

	return true, nil
}

func (sink *Sync) ProcessRetries(root string) bool {

	to_retry := sink.Retries.Length()

	if to_retry > 0 {

		scheduled_f := float64(sink.Scheduled)
		retry_f := float64(to_retry)

		pct := (retry_f / scheduled_f) * 100.0

		if pct > sink.MaxRetries {
			sink.Logger.Warning("E_EXCESSIVE_ERRORS, %f percent of scheduled processes failed thus undermining our faith that they will work now...", pct)
			return false
		}

		sink.Logger.Info("There are %d failed requests that will now be retried", to_retry)

		wg := new(sync.WaitGroup)

		for sink.Retries.Length() > 0 {

			r, ok := sink.Retries.Pop()

			if !ok {
				sink.Logger.Error("Failed to pop retries because... computers?")
				break
			}

			source := r.StringValue()

			go func(source string, root string, wg *sync.WaitGroup) {

				atomic.AddInt64(&sink.Scheduled, 1)

				sink.WorkPool.SendWork(func() {

					atomic.AddInt64(&sink.Retried, 1)

					sink.Logger.Info("Retry syncing %s", source)

					sink.SyncFile(source, root, wg)

					atomic.AddInt64(&sink.Completed, 1)
				})

			}(source, root, wg)
		}

		wg.Wait()
	}

	return true
}

func (sink *Sync) MonitorStatus() {

	go func() {

		t0 := time.Now()

		for {

			rpt := sink.StatusReport()
			ttp := time.Since(t0)

			sink.Logger.Info("%s Time %v", rpt, ttp)

			time.Sleep(10 * time.Second)

			if sink.Scheduled == sink.Completed {
				break
			}
		}

		sink.Logger.Info(sink.StatusReport())
		sink.Logger.Info("Monitoring complete")
	}()
}

func (sink *Sync) StatusReport() string {
	return fmt.Sprintf("Scheduled %d Completed %d Success %d Error %d Skipped %d Retried %d Goroutines %d",
		sink.Scheduled, sink.Completed, sink.Success, sink.Error, sink.Skipped, sink.Retried, runtime.NumGoroutine())
}
