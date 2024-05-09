package csi

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/longhorn/go-iscsi-helper/util"
	"github.com/pkg/errors"
	ctlv1 "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/mount-utils"
	utilmount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
)

var hostUtil = hostutil.NewHostUtil()

const (
	ProcPath = "/proc"
)

type NodeServer struct {
	namespace  string
	coreClient ctlv1.Interface
	virtClient kubecli.KubevirtClient
	nodeID     string
	caps       []*csi.NodeServiceCapability
	vip        string
}

func NewNodeServer(coreClient ctlv1.Interface, virtClient kubecli.KubevirtClient, nodeID string, namespace string, vip string) *NodeServer {
	return &NodeServer{
		coreClient: coreClient,
		virtClient: virtClient,
		nodeID:     nodeID,
		namespace:  namespace,
		caps: getNodeServiceCapabilities(
			[]csi.NodeServiceCapability_RPC_Type{
				csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
				csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME, // we need this to call stage/unstage
			}),
		vip: vip,
	}
}

// NodePublishVolume will mount the volume /dev/<hot_plug_device> to target_path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodePublishVolume req: %v", req)

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing target path in request")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing volume capability in request")
	}

	volAccessMode := volumeCapability.GetAccessMode().GetMode()
	if volAccessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		return ns.nodePublishRWXVolume(ctx, req, targetPath, volumeCapability)
	} else if volAccessMode == csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
		return ns.nodePublishRWOVolume(ctx, req, targetPath, volumeCapability)
	}
	return nil, status.Error(codes.InvalidArgument, "Invalid Access Mode, neither RWX nor RWO")

	//vmi, err := ns.virtClient.VirtualMachineInstance(ns.namespace).Get(ns.nodeID, &metav1.GetOptions{})
	//if err != nil {
	//	return nil, status.Errorf(codes.Internal, "Failed to get VMI %v: %v", ns.nodeID, err)
	//}
	//var hotPlugDiskReady bool
	//for _, volStatus := range vmi.Status.VolumeStatus {
	//	if volStatus.Name == req.VolumeId && volStatus.HotplugVolume != nil && volStatus.Phase == kubevirtv1.VolumeReady && volStatus.Target != "" {
	//		hotPlugDiskReady = true
	//		break
	//	}
	//}

	//if !hotPlugDiskReady {
	//	return nil, status.Errorf(codes.Aborted, "The hot-plug volume %s is not ready", req.GetVolumeId())
	//}

	//// Find hotplug disk on VM node. It can be different from volStatus.Target.
	//devicePath, err := getDevicePathByVolumeID(req.VolumeId)
	//logrus.Debugf("getDevicePathByVolumeID: %v,%v", devicePath, err)
	//if err != nil {
	//	return nil, status.Errorf(codes.Internal, "Failed to get device path for volume %v: %v", req.VolumeId, err)
	//}

	//mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
	//if volumeCapability.GetBlock() != nil {
	//	logrus.Infof("[VICENTE DBG]: handle the block volume %s", devicePath)
	//	return ns.nodePublishBlockVolume(req.GetVolumeId(), devicePath, targetPath, mounter)
	//} else if volumeCapability.GetMount() != nil {
	//	logrus.Infof("[VICENTE DBG]: Mounting volume %s to %s", devicePath, targetPath)
	//	// mounter assumes ext4 by default
	//	fsType := volumeCapability.GetMount().GetFsType()
	//	if fsType == "" {
	//		fsType = "ext4"
	//	}

	//	return ns.nodePublishMountVolume(req.GetVolumeId(), devicePath, targetPath,
	//		fsType, volumeCapability.GetMount().GetMountFlags(), mounter)
	//}
	//return nil, status.Error(codes.InvalidArgument, "Invalid volume capability, neither Mount nor Block")
}

func (ns *NodeServer) nodePublishRWXVolume(_ context.Context, req *csi.NodePublishVolumeRequest, targetPath string, _ *csi.VolumeCapability) (*csi.NodePublishVolumeResponse, error) {

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	// make sure the target path status (mounted, corrupted, not exist)
	mounterInst := utilmount.New("")
	notMounted, err := mount.IsNotMountPoint(mounterInst, targetPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(targetPath, 0750); err != nil {
			return nil, status.Errorf(codes.Internal, "Could not create target dir %s: %v", targetPath, err)
		}
	}
	// Already mounted, do nothing
	if !notMounted {
		logrus.Infof("[VICENTE DBG]: target path %s is already mounted", targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	logrus.Infof("[VICENTE DBG]: stagingTargetPath: %s, targetPath: %s", stagingTargetPath, targetPath)
	mountOptions := []string{"bind"}
	if err := mounterInst.Mount(stagingTargetPath, targetPath, "", mountOptions); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind mount volume %s to target path %s: %v", req.GetVolumeId(), targetPath, err)
	}

	return &csi.NodePublishVolumeResponse{}, nil

}

func (ns *NodeServer) nodePublishRWOVolume(_ context.Context, req *csi.NodePublishVolumeRequest, targetPath string, volCaps *csi.VolumeCapability) (*csi.NodePublishVolumeResponse, error) {
	vmi, err := ns.virtClient.VirtualMachineInstance(ns.namespace).Get(ns.nodeID, &metav1.GetOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get VMI %v: %v", ns.nodeID, err)
	}
	var hotPlugDiskReady bool
	for _, volStatus := range vmi.Status.VolumeStatus {
		if volStatus.Name == req.VolumeId && volStatus.HotplugVolume != nil && volStatus.Phase == kubevirtv1.VolumeReady && volStatus.Target != "" {
			hotPlugDiskReady = true
			break
		}
	}

	if !hotPlugDiskReady {
		return nil, status.Errorf(codes.Aborted, "The hot-plug volume %s is not ready", req.GetVolumeId())
	}

	// Find hotplug disk on VM node. It can be different from volStatus.Target.
	devicePath, err := getDevicePathByVolumeID(req.VolumeId)
	logrus.Debugf("getDevicePathByVolumeID: %v,%v", devicePath, err)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get device path for volume %v: %v", req.VolumeId, err)
	}

	mounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: utilexec.New()}
	if volCaps.GetBlock() != nil {
		logrus.Infof("[VICENTE DBG]: handle the block volume %s", devicePath)
		return ns.nodePublishBlockVolume(req.GetVolumeId(), devicePath, targetPath, mounter)
	} else if volCaps.GetMount() != nil {
		logrus.Infof("[VICENTE DBG]: Mounting volume %s to %s", devicePath, targetPath)
		// mounter assumes ext4 by default
		fsType := volCaps.GetMount().GetFsType()
		if fsType == "" {
			fsType = "ext4"
		}

		return ns.nodePublishMountVolume(req.GetVolumeId(), devicePath, targetPath,
			fsType, volCaps.GetMount().GetMountFlags(), mounter)
	}
	return nil, status.Error(codes.InvalidArgument, "Invalid volume capability, neither Mount nor Block")
}

func getDevicePathByVolumeID(volumeID string) (string, error) {
	var target string
	files, err := os.ReadDir(deviceByIDDirectory)
	if err != nil {
		return "", err
	}
	for _, file := range files {
		if strings.Contains(file.Name(), volumeID) {
			target, err = filepath.EvalSymlinks(filepath.Join(deviceByIDDirectory, file.Name()))
			if err != nil {
				return "", err
			}
			break
		}
	}
	if target == "" {
		return "", fmt.Errorf("no matching disk for volume %v", volumeID)
	}
	return target, nil
}

func (ns *NodeServer) nodePublishBlockVolume(volumeName, devicePath, targetPath string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	targetDir := filepath.Dir(targetPath)
	exists, err := hostUtil.PathExists(targetDir)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !exists {
		if err := makeDir(targetDir); err != nil {
			return nil, status.Errorf(codes.Internal, "Could not create dir %q: %v", targetDir, err)
		}
	}
	if err = makeFile(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Error in making file %v", err)
	}

	if err := mounter.Mount(devicePath, targetPath, "", []string{"bind"}); err != nil {
		if removeErr := os.Remove(targetPath); removeErr != nil {
			return nil, status.Errorf(codes.Internal, "Could not remove mount target %q: %v", targetPath, err)
		}
		return nil, status.Errorf(codes.Internal, "Could not mount %q at %q: %v", devicePath, targetPath, err)
	}
	logrus.Debugf("NodePublishVolume: done BlockVolume %s", volumeName)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeServer) nodePublishMountVolume(volumeName, devicePath, targetPath, fsType string, mountFlags []string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	// It's used to check if a directory is a mount point and it will create the directory if not exist. Hence this target path cannot be used for block volume.
	notMnt, err := isLikelyNotMountPointAttach(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		logrus.Debugf("NodePublishVolume: the volume %s has been mounted", volumeName)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := mounter.FormatAndMount(devicePath, targetPath, fsType, mountFlags); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Debugf("NodePublishVolume: done MountVolume %s", volumeName)

	return &csi.NodePublishVolumeResponse{}, nil
}

func isLikelyNotMountPointAttach(targetpath string) (bool, error) {
	notMnt, err := mount.New("").IsLikelyNotMountPoint(targetpath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(targetpath, 0750)
			if err == nil {
				notMnt = true
			}
		}
	}
	return notMnt, err
}

func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodeUnpublishVolume req: %v", req)

	if req.GetVolumeId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing volume ID in request")
	}

	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing target path in request")
	}

	mounter := mount.New("")
	for {
		if err := mounter.Unmount(targetPath); err != nil {
			if strings.Contains(err.Error(), "not mounted") ||
				strings.Contains(err.Error(), "no mount point specified") {
				break
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if notMnt {
			break
		}
		logrus.Debugf("There are multiple mount layers on mount point %v, will unmount all mount layers for this mount point", targetPath)
	}

	if err := mount.CleanupMountPoint(targetPath, mounter, false); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Infof("NodeUnpublishVolume: unmounted volume %s from path %s", req.GetVolumeId(), targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volCaps := req.GetVolumeCapability()
	if volCaps == nil {
		return nil, status.Error(codes.InvalidArgument, "Missing volume capability in request")
	}

	volAccessMode := volCaps.GetAccessMode().GetMode()
	if volAccessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
		return ns.nodeStageRWXVolume(req)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) nodeStageRWXVolume(req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	logrus.Infof("NodeStageVolume is called with req %+v", req)

	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	uri, err := url.Parse(ns.vip)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "parsing endpoing failed")
	}

	//defaultMountOptions := []string{
	//	"vers=4.2",
	//	"noresvport",
	//	//"sync",    // sync mode is prohibitively expensive on the client, so we allow for host defaults
	//	//"intr",
	//	//"hard",
	//	//"softerr", // for this release we use soft mode, so we can always cleanup mount points
	//	"timeo=600", // This is tenths of a second, so a 60 second timeout, each retrans the timeout will be linearly increased, 60s, 120s, 240s, 480s, 600s(max)
	//	"retrans=5", // We try the io operation for a total of 5 times, before failing
	//}

	// default entry point is VIP
	server := uri.Hostname()
	volName := req.GetVolumeId()
	export := fmt.Sprintf("%s:/%s", server, volName)
	logrus.Infof("[VICENTE DBG]: export path: %s", export)
	nspace := util.GetHostNamespacePath("/proc")
	logrus.Infof("[VICENTE DBG]: nspace: %s", nspace)
	// try to mount a global path on the node to the staging target path
	//mountArgs := "--mount=" + filepath.Join(nspace, "mnt")
	netArgs := "--net=" + filepath.Join(nspace, "net")
	//args := []string{"--target", "1", "--mount", "--uts", "--ipc", "--net", "--pid"}
	args := []string{netArgs}
	args = append(args, "--")
	args = append(args, "mount")
	args = append(args, "-t", "nfs", "-o", "vers=4.2,noresvport,timeo=600,retrans=5", export, stagingTargetPath)
	logrus.Infof("[VICENTE DBG]: args: %v", args)
	logrus.Infof("[VICENTE DBG]: Mounting volume %s to %s", req.VolumeId, stagingTargetPath)
	cmd := exec.Command("nsenter", args...)
	_, err = cmd.CombinedOutput()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not mount %v for global path: %v", export, err)
	}
	//executeArgs := executor.GetExecuteBinWithNS()
	//logrus.Infof("[VICENTE DBG]: executeBin: %s", executeBin)
	//mounter := utilmount.New(executeBin)
	//logrus.Infof("[VICENTE DBG]: Mounting volume %s to %s", req.VolumeId, stagingTargetPath)
	//if _, err := executor.Execute("mount", []string{export, "-t nfs -o vers=4.2,noresvport,timeo=600,retrans=5", stagingTargetPath}); err != nil {
	//	//if err := mounter.Mount(export, stagingTargetPath, "nfs", defaultMountOptions); err != nil {
	//	return nil, status.Errorf(codes.Internal, "Could not mount %v for global path: %v", export, err)
	//}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	stagingTargetPath := req.GetStagingTargetPath()
	if stagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path missing in request")
	}
	mounterInst := utilmount.New("")
	notMounted, _ := mount.IsNotMountPoint(mounterInst, stagingTargetPath)
	if notMounted {
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	if err := mounterInst.Unmount(stagingTargetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not unmount %v: %v", stagingTargetPath, err)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing volume ID in request")
	}

	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "Missing volume path in request")
	}

	volumePath := req.GetVolumePath()
	isBlockVolume, err := isBlockDevice(volumePath)
	if err != nil {
		// ENOENT means the volumePath does not exist
		// See https://man7.org/linux/man-pages/man2/stat.2.html for details.
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume path %v is not mounted", volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to check volume mode for volume path %v: %v", volumePath, err)
	}

	pvc, err := ns.coreClient.PersistentVolumeClaim().Get(ns.namespace, req.VolumeId, metav1.GetOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get PVC %v:%v", req.VolumeId, err)
	}
	if isBlockVolume {
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				{
					Total: pvc.Status.Capacity.Storage().Value(),
					Unit:  csi.VolumeUsage_BYTES,
				},
			},
		}, nil
	}

	stats, err := getFilesystemStatistics(volumePath)
	if err != nil {
		// ENOENT means the volumePath does not exist
		// See http://man7.org/linux/man-pages/man2/statfs.2.html for details.
		if errors.Is(err, unix.ENOENT) {
			return nil, status.Errorf(codes.NotFound, "volume path %v is not mounted", volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to retrieve capacity statistics for volume path %v: %v", volumePath, err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: stats.availableBytes,
				Total:     stats.totalBytes,
				Used:      stats.usedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: stats.availableInodes,
				Total:     stats.totalInodes,
				Used:      stats.usedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
	}, nil
}

func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: ns.caps,
	}, nil
}

func getNodeServiceCapabilities(cs []csi.NodeServiceCapability_RPC_Type) []*csi.NodeServiceCapability {
	var nscs = make([]*csi.NodeServiceCapability, len(cs))

	for _, cap := range cs {
		logrus.Infof("Enabling node service capability: %v", cap.String())
		nscs = append(nscs, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return nscs
}
