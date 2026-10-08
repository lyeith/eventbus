package lambda

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"time"
)

const maxPhaseFrame = 1024

// Managed runtimes use fd6 READY / fd7 ACK. Native fd3 results and opt-in
// fd4/fd5 stack collection remain independent, including when collection is off.
type managedPhaseChannel struct {
	readyReader, readyWriter, ackReader, ackWriter, placeholder *os.File
	done                                                        chan struct{}
	readErr                                                     error
}

func newManagedPhaseChannel(command *exec.Cmd) (_ *managedPhaseChannel, err error) {
	channel := &managedPhaseChannel{}
	defer func() {
		if err != nil {
			channel.close()
		}
	}()
	channel.readyReader, channel.readyWriter, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	channel.ackReader, channel.ackWriter, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	if len(command.ExtraFiles) == 1 {
		channel.placeholder, err = os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		command.ExtraFiles = append(command.ExtraFiles, channel.placeholder, channel.placeholder)
	}
	command.ExtraFiles = append(command.ExtraFiles, channel.readyWriter, channel.ackReader)
	return channel, nil
}

func (channel *managedPhaseChannel) childStarted(phase *runtimePhase, started bool) {
	_ = channel.readyWriter.Close()
	_ = channel.ackReader.Close()
	if channel.placeholder != nil {
		_ = channel.placeholder.Close()
	}
	if !started {
		return
	}
	channel.done = make(chan struct{})
	go func() {
		defer close(channel.done)
		defer channel.ackWriter.Close()
		// EOF is not a live message boundary: launchers can retain duplicate FDs.
		data, err := bufio.NewReaderSize(channel.readyReader, maxPhaseFrame+1).ReadSlice('\n')
		channel.readErr = err
		if err != nil {
			// EOF before readiness is a native initialization failure/early exit.
			// Its fd3 envelope remains authoritative after the actual process join.
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrDeadlineExceeded) {
				phase.cancel(errPhaseProtocol)
			}
			return
		}
		var ready struct {
			Version int  `json:"version"`
			Ready   bool `json:"ready"`
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		var extra any
		if len(data) > maxPhaseFrame || decoder.Decode(&ready) != nil || decoder.Decode(&extra) != io.EOF || ready.Version != 1 || !ready.Ready {
			phase.cancel(errPhaseProtocol)
			return
		}
		deadline, err := phase.beginInvoke()
		if err != nil {
			return
		}
		ack, _ := json.Marshal(struct {
			Version    int   `json:"version"`
			DeadlineMS int64 `json:"deadline_ms"`
		}{1, deadline.UnixMilli()})
		if _, err := channel.ackWriter.Write(append(ack, '\n')); err != nil {
			phase.cancel(errPhaseProtocol)
		}
	}()
}

// join follows actual process/group cleanup, bounding a retained control FD.
// Protocol errors are native function failures; retained ownership is separate.
func (channel *managedPhaseChannel) join() error {
	if channel.done == nil {
		return nil
	}
	select {
	case <-channel.done:
	default:
		if err := channel.readyReader.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			_ = channel.readyReader.Close()
			<-channel.done
			return err
		}
		<-channel.done
	}
	if errors.Is(channel.readErr, os.ErrDeadlineExceeded) {
		return errors.New("runtime readiness pipe retained after process cleanup")
	}
	return nil
}

func (channel *managedPhaseChannel) close() {
	for _, file := range []*os.File{channel.readyReader, channel.readyWriter, channel.ackReader, channel.ackWriter, channel.placeholder} {
		if file != nil {
			_ = file.Close()
		}
	}
}
