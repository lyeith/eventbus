package firehose

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamOwnedTimeZonePreservesDSTGroupingAndObjectPrefixes(t *testing.T) {
	manager := newTestFirehoseManager(t)
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	config := partitionConfig("prepared-zone")
	config.CustomTimeZone = "America/New_York"
	config.CompressionFormat = "UNCOMPRESSED"
	var builtLocation *time.Location
	manager.buildObject = func(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
		builtLocation = location
		return makeObject(ctx, config, location, records)
	}
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	stream.cancel()
	<-stream.done
	location := stream.location
	require.NotNil(t, location)
	require.Equal(t, "America/New_York", location.String())

	// Neither original configuration nor detached native readback can replace
	// the location used by an existing stream.
	config.CustomTimeZone = "Asia/Singapore"
	snapshot, ok := manager.Snapshot(stream.Name)
	require.True(t, ok)
	require.Equal(t, "America/New_York", snapshot.Config.CustomTimeZone)
	snapshot.Config.CustomTimeZone = "UTC"
	for _, test := range []struct {
		name, utc, localHour string
		offset               int
	}{
		{"before spring jump", "2024-03-10T06:59:59Z", "2024/03/10/01/", -5 * 60 * 60},
		{"after spring jump", "2024-03-10T07:00:00Z", "2024/03/10/03/", -4 * 60 * 60},
		{"first fall hour", "2024-11-03T05:30:00Z", "2024/11/03/01/", -4 * 60 * 60},
		{"repeated fall hour", "2024-11-03T06:30:00Z", "2024/11/03/01/", -5 * 60 * 60},
	} {
		t.Run(test.name, func(t *testing.T) {
			arrived, err := time.Parse(time.RFC3339, test.utc)
			require.NoError(t, err)
			_, offset := arrived.In(location).Zone()
			require.Equal(t, test.offset, offset)
			for _, data := range [][]byte{[]byte(`{"customer_id":"north","id":1}`), []byte("malformed partition JSON")} {
				record := prepareRecord(t.Context(), stream.config, stream.location, stream.metadata, data, arrived)
				prefix := "tenant=north/" + test.localHour
				if record.errorType != "" {
					prefix = "errors/dynamic-partitioning-failed/" + test.localHour
					var diagnostic struct{ RawData string }
					require.NoError(t, json.Unmarshal(record.data, &diagnostic))
					require.Equal(t, base64.StdEncoding.EncodeToString(data), diagnostic.RawData)
				}
				require.Equal(t, prefix, record.group)
				objects, selected, err := buildBufferedObjects(t.Context(), stream, []bufferedRecord{record}, true)
				require.NoError(t, err)
				require.True(t, selected[prefix])
				require.Len(t, objects, 1)
				require.True(t, strings.HasPrefix(objects[0].key, prefix), objects[0].key)
				require.Equal(t, record.data, objects[0].data)
				require.Equal(t, len(data), objects[0].originalBytes)
				require.Same(t, location, builtLocation, "the object builder uses the grouping location")
			}
		})
	}
	next, _ := manager.Snapshot(stream.Name)
	require.Equal(t, "America/New_York", next.Config.CustomTimeZone)
	require.Same(t, location, stream.location)
}

func TestStreamTimeZoneValidationAndRecreationOwnNewLocation(t *testing.T) {
	manager := newTestFirehoseManager(t)
	config := StreamConfig{
		Name: "zone-lifetime", BucketARN: "arn:aws:s3:::bucket",
		RoleARN: "arn:aws:iam::000000000000:role/firehose", CompressionFormat: "UNCOMPRESSED",
		CustomTimeZone: "not/a/zone", BufferingHints: BufferingHints{SizeInMBs: 1, IntervalInSeconds: 60},
	}
	_, err := manager.CreateConfiguredStream(t.Context(), config)
	require.ErrorContains(t, err, "invalid CustomTimeZone")
	require.Nil(t, manager.GetStream(config.Name))
	config.CustomTimeZone = "America/New_York"
	original, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	require.NoError(t, manager.DeleteStream(t.Context(), config.Name))
	config.CustomTimeZone = "Asia/Singapore"
	recreated, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	require.NotSame(t, original, recreated)
	require.NotSame(t, original.location, recreated.location)
	require.Equal(t, "America/New_York", original.location.String())
	require.Equal(t, "Asia/Singapore", recreated.location.String())
	_, err = manager.AcceptRecordBatch(t.Context(), original, [][]byte{[]byte("retired stream")})
	require.ErrorContains(t, err, "unavailable")
	legacy, err := manager.CreateStream("legacy-zone", "bucket", "", "", 1, 60)
	require.NoError(t, err)
	require.Same(t, time.UTC, legacy.location)
}
