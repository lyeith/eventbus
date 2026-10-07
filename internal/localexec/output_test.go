package localexec

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundedOutputConsumesBytesWithoutAcceptingTruncation(t *testing.T) {
	output := NewBoundedOutput(3)
	copied, err := io.Copy(output, io.LimitReader(strings.NewReader("12345"), 5))
	require.NoError(t, err)
	require.EqualValues(t, 5, copied)
	require.Equal(t, "123", output.String())
	require.True(t, output.Overflowed())
	written, err := output.Write([]byte("more"))
	require.NoError(t, err)
	require.Equal(t, 4, written)
	require.Equal(t, "123", output.String())
	require.Equal(t, 3, output.Len())
}

func TestBoundedOutputDetectsBytesAfterExactCap(t *testing.T) {
	output := NewBoundedOutput(3)
	for _, data := range []string{"1", "23", ""} {
		written, err := output.Write([]byte(data))
		require.NoError(t, err)
		require.Equal(t, len(data), written)
		require.False(t, output.Overflowed())
	}
	written, err := output.Write([]byte("4"))
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.True(t, output.Overflowed())
	require.Equal(t, "123", output.String())
}

func TestBoundedOutputSnapshotsDoNotShareMutableBytes(t *testing.T) {
	output := NewBoundedOutput(4)
	input := []byte("123")
	_, err := output.Write(input)
	require.NoError(t, err)
	input[0] = 'x'
	snapshot := output.Bytes()
	snapshot[1] = 'y'
	require.Equal(t, "123", output.String())
	require.False(t, output.Overflowed())
}

func TestBoundedOutputZeroLimitStillDrains(t *testing.T) {
	output := NewBoundedOutput(0)
	written, err := output.Write(nil)
	require.NoError(t, err)
	require.Zero(t, written)
	require.False(t, output.Overflowed())
	copied, err := io.Copy(output, strings.NewReader("discarded"))
	require.NoError(t, err)
	require.EqualValues(t, len("discarded"), copied)
	require.Zero(t, output.Len())
	require.True(t, output.Overflowed())
}
