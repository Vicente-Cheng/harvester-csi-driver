package csi

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/procfs"
	"golang.org/x/sys/unix"
)

const (
	deviceByIDDirectory = "/dev/disk/by-id/"
	driverName          = "driver.harvesterhci.io"

	NSBinary          = "nsenter"
	cmdTimeoutDefault = 180 * time.Second // 3 minutes by default
	cmdTimeoutNone    = 0 * time.Second   // no timeout

	DockerdProcess        = "dockerd"
	ContainerdProcess     = "containerd"
	ContainerdProcessShim = "containerd-shim"
)

type Executor struct {
	namespace  string
	cmdTimeout time.Duration
}

type volumeFilesystemStatistics struct {
	availableBytes int64
	totalBytes     int64
	usedBytes      int64

	availableInodes int64
	totalInodes     int64
	usedInodes      int64
}

// isBlockDevice return true if volumePath file is a block device, false otherwise.
func isBlockDevice(volumePath string) (bool, error) {
	var stat unix.Stat_t
	// See https://man7.org/linux/man-pages/man2/stat.2.html for details
	err := unix.Stat(volumePath, &stat)
	if err != nil {
		return false, err
	}

	// See https://man7.org/linux/man-pages/man7/inode.7.html for detail
	if (stat.Mode & unix.S_IFMT) == unix.S_IFBLK {
		return true, nil
	}

	return false, nil
}

func getFilesystemStatistics(volumePath string) (*volumeFilesystemStatistics, error) {
	var statfs unix.Statfs_t
	// See http://man7.org/linux/man-pages/man2/statfs.2.html for details.
	err := unix.Statfs(volumePath, &statfs)
	if err != nil {
		return nil, err
	}

	volStats := &volumeFilesystemStatistics{
		availableBytes: int64(statfs.Bavail) * int64(statfs.Bsize),
		totalBytes:     int64(statfs.Blocks) * int64(statfs.Bsize),
		usedBytes:      (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize),

		availableInodes: int64(statfs.Ffree),
		totalInodes:     int64(statfs.Files),
		usedInodes:      int64(statfs.Files) - int64(statfs.Ffree),
	}

	return volStats, nil
}

// makeDir creates a new directory.
// If pathname already exists as a directory, no error is returned.
// If pathname already exists as a file, an error is returned.
func makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

// makeFile creates an empty file.
// If pathname already exists, whether a file or directory, no error is returned.
func makeFile(pathname string) error {
	f, err := os.OpenFile(pathname, os.O_CREATE, os.FileMode(0644))
	if f != nil {
		f.Close()
	}
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func NewExecutor() *Executor {
	return &Executor{
		namespace:  "",
		cmdTimeout: cmdTimeoutDefault,
	}
}

func NewExecutorWithNS(ns string) (*Executor, error) {
	exec := NewExecutor()
	exec.namespace = ns

	// test if nsenter is available
	if _, err := execute(NSBinary, []string{"-V"}, cmdTimeoutNone); err != nil {
		return nil, errors.Wrap(err, "cannot find nsenter for namespace switching")
	}
	return exec, nil
}

func (exec *Executor) SetTimeout(timeout time.Duration) {
	exec.cmdTimeout = timeout
}

func (exec *Executor) GetExecuteBinWithNS() string {
	cmdArgs := []string{
		"--target=1",
		"--mount=" + filepath.Join(exec.namespace, "mnt"),
		"--net=" + filepath.Join(exec.namespace, "net"),
		"--ipc=" + filepath.Join(exec.namespace, "ipc"),
	}
	executeBin := append([]string{NSBinary}, cmdArgs...)
	return strings.Join(executeBin, " ")
}

func (exec *Executor) Execute(cmd string, args []string) (string, error) {
	command := cmd
	cmdArgs := args
	if exec.namespace != "" {
		cmdArgs = []string{
			"--mount=" + filepath.Join(exec.namespace, "mnt"),
			"--net=" + filepath.Join(exec.namespace, "net"),
			"--ipc=" + filepath.Join(exec.namespace, "ipc"),
			cmd,
		}
		command = NSBinary
		cmdArgs = append(cmdArgs, args...)
	}
	return execute(command, cmdArgs, exec.cmdTimeout)
}

func execute(command string, args []string, timeout time.Duration) (string, error) {
	cmd := exec.Command(command, args...)

	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr

	timer := time.NewTimer(cmdTimeoutNone)
	if timeout != cmdTimeoutNone {
		// add timer to kill the process if timeout
		timer = time.AfterFunc(timeout, func() {
			cmd.Process.Kill()
		})
	}
	defer timer.Stop()

	if err := cmd.Run(); err != nil {
		return "", errors.Wrapf(err, "failed to execute: %v %v, output %s, stderr %s",
			command, args, output.String(), stderr.String())
	}

	return output.String(), nil
}

func getPidProc(hostProcPath string, pid int) (*procfs.Proc, error) {
	fs, err := procfs.NewFS(hostProcPath)
	if err != nil {
		return nil, err
	}
	proc, err := fs.Proc(pid)
	if err != nil {
		return nil, err
	}
	return &proc, nil
}

func getSelfProc(hostProcPath string) (*procfs.Proc, error) {
	fs, err := procfs.NewFS(hostProcPath)
	if err != nil {
		return nil, err
	}
	proc, err := fs.Self()
	if err != nil {
		return nil, err
	}
	return &proc, nil
}

func findAncestorByName(hostProcPath string, ancestorProcess string) (*procfs.Proc, error) {
	proc, err := getSelfProc(hostProcPath)
	if err != nil {
		return nil, err
	}

	for {
		st, err := proc.Stat()
		if err != nil {
			return nil, err
		}
		if st.Comm == ancestorProcess {
			return proc, nil
		}
		if st.PPID == 0 {
			break
		}
		proc, err = getPidProc(hostProcPath, st.PPID)
		if err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("failed to find the ancestor process: %s", ancestorProcess)
}

func GetHostNamespacePath(hostProcPath string) string {
	containerNames := []string{DockerdProcess, ContainerdProcess, ContainerdProcessShim}
	for _, name := range containerNames {
		proc, err := findAncestorByName(hostProcPath, name)
		if err == nil {
			return fmt.Sprintf("%s/%d/ns/", hostProcPath, proc.PID)
		}
	}
	return fmt.Sprintf("%s/%d/ns/", hostProcPath, 1)
}
