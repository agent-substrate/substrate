// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/versionlabel"
)

// DeleteOptions are the knobs of the control-plane delete commands.
type DeleteOptions struct {
	// KeepNodeState leaves nodepath.BasePath on the nodes.
	KeepNodeState bool
	// WipeNodeCaches wipes nodeStateCaches with the rest of
	// nodepath.BasePath, rather than leaving them.
	WipeNodeCaches bool
}

const (
	nodeStateWipeName = "ate-node-state-wipe"
	// kube-system rather than the install namespace, which the teardown
	// deletes, and rather than default, where Pod Security admission may
	// forbid hostPath. The CSI node plugins run there for the same reason.
	nodeStateWipeNamespace = metav1.NamespaceSystem
	// Pinned to the tag's multi-arch index: this runs as root against every
	// node's state, so the image is the one reviewed rather than whatever the
	// tag points at today.
	nodeStateWipeImage    = "busybox:1.36.1@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
	nodeStateMountPath    = "/host-state"
	nodeStateWipeDoneFile = "/tmp/done"
)

// nodeStateCaches are the content-addressed caches directly under
// nodepath.BasePath: image layers, and the sandbox runtime assets. Everything
// in them is named by its digest, so none of it belongs to one install, and
// wiping it only makes the next install fetch the same bytes again.
//
// image-cache must agree with ateletpath.ImageCacheDir, which ate-setup cannot
// import.
var nodeStateCaches = []string{"image-cache", filepath.Base(nodepath.StaticFilesDir)}

// nodeStateWipeKeep names the entries directly under nodepath.BasePath that
// the wipe leaves in place.
func nodeStateWipeKeep(opts DeleteOptions) []string {
	if opts.WipeNodeCaches {
		return nil
	}
	return nodeStateCaches
}

// nodeStateWipeScript defines wipe_node_state ROOT [MOUNTINFO [KEEP]], which
// empties ROOT except for the entries directly under it named in KEEP, a
// space-separated list. ROOT itself stays, since it is the pod's mount point.
//
// It does not delete through mounts. hostPath volumes are recursive bind
// mounts, so an actor's external volume still mounted under
// actors/<uid>/volumes/ is visible below ROOT, and rm -rf would delete the
// volume's contents rather than node state. -xdev cannot tell a
// same-filesystem bind mount apart, so mount points are taken from the mount
// table. They are left in place, along with the directories leading to them,
// and everything else is removed.
//
// Mount points come from field 5 of mountinfo, where spaces and the like are
// octal-escaped. That only matters for names atelet never creates.
const nodeStateWipeScript = `
wipe_node_state() {
	wipe_root=$1
	wipe_status=0
	if ! wipe_mounts=$(awk -v root="$wipe_root" 'index($5, root "/") == 1 { print $5 }' "${2:-/proc/self/mountinfo}"); then
		echo "warning: leaving $wipe_root in place: cannot read the mount table" >&2
		return 1
	fi
	wipe_dir "$wipe_root" "$3"
	return "$wipe_status"
}

wipe_dir() {
	for wipe_entry in "$1"/* "$1"/.[!.]* "$1"/..?*; do
		# A pattern that matches nothing stays literal.
		[ -e "$wipe_entry" ] || [ -L "$wipe_entry" ] || continue
		case " $2 " in *" ${wipe_entry##*/} "*) continue ;; esac
		case $(printf '%s\n' "$wipe_mounts" | awk -v e="$wipe_entry" '$0 == e { m = 1 } index($0, e "/") == 1 { p = 1 } END { print (m ? "mount" : (p ? "parent" : "none")) }') in
		mount)
			echo "warning: leaving $wipe_entry in place: still mounted" >&2
			wipe_status=1
			;;
		parent) wipe_dir "$wipe_entry" ;;
		*) rm -rf -- "$wipe_entry" || wipe_status=1 ;;
		esac
	done
}
`

// nodeStateWipeMain runs the wipe once, leaving the entries named in keep, and
// then holds the pod Ready, so that the rollout finishing means every node has
// been visited. A partial wipe is reported in the pod log and still counts as
// visited: crash-looping would only hold up the uninstall until the rollout
// timed out.
func nodeStateWipeMain(keep []string) string {
	return fmt.Sprintf(`
wipe_node_state %s /proc/self/mountinfo '%s' || echo "warning: some node state was left in place" >&2
touch %s
while :; do sleep 3600; done
`, nodeStateMountPath, strings.Join(keep, " "), nodeStateWipeDoneFile)
}

// shouldWipeNodeState decides whether node state goes with the control plane.
// It runs before anything is deleted, because the answer may be recorded in
// the install namespace.
//
// An actor's directories are garbage only once no actor record can point at
// them. The bundled PostgreSQL is deleted with the install and takes every
// record with it. An external database outlives the install, and the local
// snapshots of its PAUSED actors exist only on their nodes.
func (e *Env) shouldWipeNodeState(ctx context.Context, opts DeleteOptions) bool {
	if opts.KeepNodeState {
		log.Infof("Keeping %s on the nodes: --keep-node-state", nodepath.BasePath)
		return false
	}
	plan, err := e.planPostgres(ctx)
	if err != nil {
		log.Warnf("Keeping %s on the nodes: cannot tell whether the database outlives the install: %v", nodepath.BasePath, err)
		return false
	}
	if !plan.bundled {
		log.Infof("Keeping %s on the nodes: the external database (%s) outlives the install", nodepath.BasePath, plan.external)
		return false
	}
	return true
}

// wipeNodeState empties nodepath.BasePath on every node atelet ran on, except
// for the entries named in keep.
//
// Deleting the control plane does not touch that directory, so without this
// the data outlives the software. Actor directories and local snapshots
// survive an uninstall and a reinstall, and the new atelets start on top of
// them. The wipe runs after the namespace is gone, so no atelet or worker is
// still writing. It runs before the nodes are unlabeled, because the label is
// how it finds them.
//
// A DaemonSet is the only way to reach every node's filesystem: atelet is
// distroless and, by this point, deleted.
func (e *Env) wipeNodeState(ctx context.Context, keep []string) error {
	log.Stepf("wipe_node_state (%s)", nodepath.BasePath)
	if len(keep) > 0 {
		log.Infof("Leaving the content-addressed caches under %s (%s); --wipe-node-caches wipes them too", nodepath.BasePath, strings.Join(keep, ", "))
	}
	daemonSets := e.Kube.Typed.AppsV1().DaemonSets(nodeStateWipeNamespace)
	// AlreadyExists is a leftover from an interrupted run. It does the same
	// job, with that run's choice of caches to keep, so wait on it and delete
	// it like a fresh one.
	if _, err := daemonSets.Create(ctx, nodeStateWipeDaemonSet(keep), metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("while creating daemonset %s/%s: %w", nodeStateWipeNamespace, nodeStateWipeName, err)
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDaemonSet, nodeStateWipeNamespace, nodeStateWipeName, e.Cfg.WaitTimeout(NodeStateWipeTimeout)); err != nil {
		log.Warnf("The node state wipe did not finish on every node, so some may keep %s: %v", nodepath.BasePath, err)
	}
	if err := daemonSets.Delete(ctx, nodeStateWipeName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("while deleting daemonset %s/%s: %w", nodeStateWipeNamespace, nodeStateWipeName, err)
	}
	return nil
}

// nodeStateWipeDaemonSet runs nodeStateWipeScript, leaving the entries named
// in keep, on every node that carries the substrate version label, which is
// every node the install deployed atelet to. It tolerates every taint so that
// no such node is skipped.
func nodeStateWipeDaemonSet(keep []string) *appsv1.DaemonSet {
	labels := map[string]string{"app": nodeStateWipeName}
	hostPathType := corev1.HostPathDirectoryOrCreate
	gracePeriod := int64(1)
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeStateWipeName,
			Namespace: nodeStateWipeNamespace,
			Labels:    labels,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			// RolloutStatus takes any other strategy as done at once; this
			// one makes it wait for every pod's readiness, i.e. its wipe.
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{corev1.LabelOSStable: "linux"},
					Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{{
								MatchExpressions: []corev1.NodeSelectorRequirement{{
									Key:      versionlabel.Key,
									Operator: corev1.NodeSelectorOpExists,
								}},
							}},
						},
					}},
					Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
					TerminationGracePeriodSeconds: &gracePeriod,
					Containers: []corev1.Container{{
						Name:    "wipe",
						Image:   nodeStateWipeImage,
						Command: []string{"/bin/sh", "-c", nodeStateWipeScript + nodeStateWipeMain(keep)},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{Command: []string{"test", "-f", nodeStateWipeDoneFile}},
							},
							InitialDelaySeconds: 1,
							PeriodSeconds:       2,
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "node-state",
							MountPath: nodeStateMountPath,
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "node-state",
						VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
							Path: nodepath.BasePath,
							Type: &hostPathType,
						}},
					}},
				},
			},
		},
	}
}
