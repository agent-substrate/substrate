package functionaltest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

func BenchmarkResumeActor(b *testing.B) {
	const numActors = 256

	ns := namespaceForTest("ns-resume-contention")
	tc := setupTest(b, ns)
	defer tc.cleanup()

	// createTemplate makes pool1 for us; widen it so one worker has room for all
	// of them. The point is that they contend, not that they are turned away for
	// lack of capacity.
	createTemplate(b, tc, ns)
	createWorkerPod(b, tc, ns, "worker-1", "node1", "pool1")
	setWorkerActorCapacity(b, tc, "pool1", numActors)

	resultLock := sync.Mutex{}
	overallErrors := 0
	overallAttempts := 0

	for b.Loop() {
		b.StopTimer()
		for i := range numActors {
			if _, err := tc.client.CreateActor(context.Background(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("id%d", i)},
				ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			}}); err != nil {
				b.Fatalf("CreateActor %d: %v", i, err)
			}
		}
		b.StartTimer()

		// Resume them all at once. Starting the goroutines is not enough to make
		// them race -- release them together so they arrive at the claim inside the
		// same window.
		var start sync.WaitGroup
		start.Add(1)
		var wg sync.WaitGroup

		for i := range numActors {
			wg.Go(func() {
				start.Wait()

				actorID := fmt.Sprintf("id%d", i)

				backoffPolicy := wait.Backoff{
					Steps:    10,
					Duration: 1 * time.Millisecond,
					Factor:   2.0,
					Jitter:   1.0,
				}

				err := wait.ExponentialBackoffWithContext(b.Context(), backoffPolicy, func(ctx context.Context) (bool, error) {
					resultLock.Lock()
					overallAttempts += 1
					resultLock.Unlock()

					_, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{
						Actor: &ateapi.ObjectRef{
							Atespace: testAtespace,
							Name:     actorID,
						},
					})
					if status.Code(err) == codes.Aborted || status.Code(err) == codes.Unavailable {
						return false, nil // Retry
					} else if err != nil {
						return false, err
					}

					return true, nil
				})

				resultLock.Lock()
				defer resultLock.Unlock()
				if err != nil {
					overallErrors += 1
				}
			})
		}
		start.Done()
		wg.Wait()

		b.StopTimer()
		for i := range numActors {
			actorName := fmt.Sprintf("id%d", i)

			actor, err := tc.client.GetActor(b.Context(), &ateapi.GetActorRequest{
				Actor: &ateapi.ObjectRef{
					Atespace: testAtespace,
					Name:     actorName,
				},
			})
			if err != nil {
				b.Fatalf("GetActor %d: %v", i, err)
			}

			_, err = tc.client.DeleteActor(context.Background(), &ateapi.DeleteActorRequest{
				Actor: &ateapi.ObjectRef{
					Atespace: actor.GetMetadata().GetAtespace(),
					Name:     actor.GetMetadata().GetName(),
				},
				Options: &ateapi.DeleteOptions{
					Uid:     actor.GetMetadata().GetUid(),
					Version: actor.GetMetadata().GetVersion(),
				},
			})
			if err != nil {
				b.Fatalf("DeleteActor %d: %v", i, err)
			}
		}
		b.StartTimer()

	}

	resultLock.Lock()
	defer resultLock.Unlock()

	// Count total attempts and report
	b.ReportMetric(float64(overallAttempts)/float64(numActors)/float64(b.N), "ResumeActor-attempts/op")
	b.ReportMetric(float64(overallErrors)/float64(numActors)/float64(b.N), "ResumeActor-hard-errors/op")
}
