package objtesting

import (
	"os"
	"testing"

	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/inmem"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

// ForeachStore runs given test using all available objstore implementations.
// For each it creates a new bucket with a random name and a cleanup function
// that deletes it after test was run.
func ForeachStore(t *testing.T, testFn func(t testing.TB, bkt objstore.Bucket)) {
	// Mandatory Inmem.
	if ok := t.Run("inmem", func(t *testing.T) {
		defer leaktest.CheckTimeout(t, 10*time.Second)()

		testFn(t, inmem.NewBucket())

	}); !ok {
		return
	}

	// Optional GCS.
	if project, ok := os.LookupEnv("GCP_PROJECT"); ok {
		bkt, closeFn, err := gcs.NewTestBucket(t, project)
		testutil.Ok(t, err)

		ok := t.Run("gcs", func(t *testing.T) {
			// TODO(bplotka): Add leaktest when we fix potential leak in GCS native client.
			testFn(t, bkt)
		})
		closeFn()
		if !ok {
			return
		}
	} else {
		t.Log("No GCP_PROJECT envvar. Skipping test against GCS")
	}

	// Optional AWS S3.
	// TODO(bplotka): Prepare environment & CI to run it automatically.
	// TODO(bplotka): Find a user with S3 AWS project ready to run this test.
	if _, ok := os.LookupEnv("S3_BUCKET"); ok {
		// TODO(bplotka): Allow taking location from envvar.
		bkt, closeFn, err := s3.NewTestBucket(t, "eu-west-1")
		testutil.Ok(t, err)

		ok := t.Run("aws s3", func(t *testing.T) {
			defer leaktest.CheckTimeout(t, 10*time.Second)()

			testFn(t, bkt)
		})
		closeFn()
		if !ok {
			return
		}
	} else {
		t.Log("No S3_BUCKET envvar. Skipping test against AWS S3")
	}
}
