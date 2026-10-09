package ssm

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSMOrderedNamesPreserveHierarchyAndRawPaginationOrder(t *testing.T) {
	store := NewSSMStore()
	// Bare and slash names are distinct parameters. Punctuation can place a
	// bare prefix before its slash equivalent in raw lexical order.
	names := []string{
		"tree/z", "/tree/nested/z", "/tree/a", "root", "/tree", "/trees/leak",
		"/tree/nested/a", "tree/a", "/tree/z", "/root", "-app/z", "/-app/a",
		".app/z", "/.app/a", "_app/a", "/_app/z", "/tree/a-more", "tree/nested/a",
		"/tree/nested/deeper/value", "tree", "-root", ".root", "/tree/nested",
	}
	for index, name := range names {
		kind := "String"
		if index%3 == 0 {
			kind = "SecureString"
		}
		require.NoError(t, store.PutParameter(name, "value:"+name, kind, false))
	}
	for _, path := range []string{"/", "/tree", "/tree/", "/tree/nested", "/-app", "/.app", "/_app", "/missing", "//tree", "tree"} {
		for _, recursive := range []bool{false, true} {
			// Compute expected names independently of the ordered index.
			prefix := strings.TrimRight(path, "/")
			if prefix == "" {
				prefix = "/"
			} else {
				prefix += "/"
			}
			expected := make([]string, 0)
			for _, name := range names {
				hierarchy := "/" + strings.TrimPrefix(name, "/")
				if remainder, found := strings.CutPrefix(hierarchy, prefix); found && remainder != "" &&
					(recursive || !strings.Contains(remainder, "/")) {
					expected = append(expected, name)
				}
			}
			sort.Strings(expected)
			for _, pageSize := range []int{1, 2, 10} {
				t.Run(fmt.Sprintf("%s/recursive_%t/page_%d", path, recursive, pageSize), func(t *testing.T) {
					actual, token := make([]string, 0), ""
					for pages := 0; ; pages++ {
						require.LessOrEqual(t, pages, len(expected), "cursor must terminate")
						page, next, err := store.ListParametersByPath(path, recursive, true, pageSize, token)
						require.NoError(t, err)
						require.LessOrEqual(t, len(page), pageSize)
						for _, parameter := range page {
							actual = append(actual, parameter.Name)
							require.Equal(t, "value:"+parameter.Name, parameter.Value)
							require.Equal(t, int64(1), parameter.Version)
							parameter.Name, parameter.Value = "mutated snapshot", "mutated snapshot"
						}
						token = next
						if token == "" {
							break
						}
						require.Len(t, page, pageSize, "a cursor requires a full page plus lookahead")
					}
					require.Equal(t, expected, actual)
				})
			}
		}
	}
	all := store.GetParametersByPath("/")
	require.Len(t, all, len(names), "fixture API uses the same index and detached plaintext snapshots")
	for _, parameter := range all {
		require.Equal(t, "value:"+parameter.Name, parameter.Value)
	}
}

func TestSSMOrderedNamesCursorTracksLiveMutations(t *testing.T) {
	store := NewSSMStore()
	for _, name := range []string{"/tree/e", "tree/d", "/tree/a", "tree/b", "/tree/c", "/unrelated/value"} {
		require.NoError(t, store.PutParameter(name, "initial:"+name, "String", false))
	}
	first, cursor, err := store.ListParametersByPath("/tree", true, true, 2, "")
	require.NoError(t, err)
	require.Equal(t, []string{"/tree/a", "/tree/c"}, []string{first[0].Name, first[1].Name})
	require.NotEmpty(t, cursor)

	// A cursor remains an exclusive raw-name boundary rather than a cached
	// snapshot, even if the boundary itself was deleted.
	require.True(t, store.DeleteParameterIfExists("/tree/c"))
	require.True(t, store.DeleteParameterIfExists("/tree/e"))
	require.False(t, store.DeleteParameterIfExists("/tree/e"))
	require.NoError(t, store.PutParameter("/tree/b", "inserted before cursor", "String", false))
	require.NoError(t, store.PutParameter("/tree/d", "inserted after cursor", "String", false))
	require.NoError(t, store.PutParameter("/tree/e", "recreated secret", "SecureString", false))
	require.NoError(t, store.PutParameter("tree/b", "overwritten after cursor", "", true))
	require.ErrorIs(t, store.PutParameter("/tree/d", "failed duplicate", "String", false), errParameterAlreadyExists)
	require.Error(t, store.PutParameter("/tree/f", "failed type", "invalid", false))

	second, next, err := store.ListParametersByPath("/tree/", true, true, 3, cursor)
	require.NoError(t, err)
	require.NotEmpty(t, next)
	require.Equal(t, []string{"/tree/d", "/tree/e", "tree/b"}, []string{second[0].Name, second[1].Name, second[2].Name})
	require.Equal(t, "recreated secret", second[1].Value)
	require.Equal(t, int64(1), second[1].Version)
	require.Equal(t, "overwritten after cursor", second[2].Value)
	require.Equal(t, int64(2), second[2].Version)
	second[1].Value = "mutated plaintext"
	last, next, err := store.ListParametersByPath("/tree", true, true, 10, next)
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, last, 1)
	require.Equal(t, "tree/d", last[0].Name)

	ciphertext, _, err := store.ListParametersByPath("/tree", true, false, 10, "")
	require.NoError(t, err)
	for _, parameter := range ciphertext {
		if parameter.Name == "/tree/e" {
			require.NotEqual(t, "recreated secret", parameter.Value)
		}
	}
	// Cursor signatures and request-option bindings remain owned by the store.
	_, _, err = store.ListParametersByPath("/tree", true, false, 3, cursor)
	require.ErrorIs(t, err, errInvalidNextToken)
	_, _, err = NewSSMStore().ListParametersByPath("/tree", true, true, 3, cursor)
	require.ErrorIs(t, err, errInvalidNextToken)

	all, next, err := store.ListParametersByPath("/tree", true, true, 10, "")
	require.NoError(t, err)
	require.Empty(t, next)
	actual := make([]string, 0, len(all))
	for _, parameter := range all {
		actual = append(actual, parameter.Name)
	}
	require.Equal(t, []string{"/tree/a", "/tree/b", "/tree/d", "/tree/e", "tree/b", "tree/d"}, actual)
}

func TestSSMOrderedNamesNonrecursiveSkipsNestedSubtrees(t *testing.T) {
	store := NewSSMStore()
	for _, prefix := range []string{"/tree", "tree", "/other"} {
		for index := 0; index < 100; index++ {
			require.NoError(t, store.PutParameter(fmt.Sprintf("%s/a/nested/p%03d", prefix, index), "nested", "String", false))
		}
	}
	for _, name := range []string{"/tree/b", "/tree/c", "tree/b", "tree/c"} {
		require.NoError(t, store.PutParameter(name, "direct", "String", false))
	}
	token, actual := "", make([]string, 0, 4)
	for pageIndex := 0; ; pageIndex++ {
		require.Less(t, pageIndex, 4)
		page, next, err := store.ListParametersByPath("/tree", false, false, 1, token)
		require.NoError(t, err)
		require.Len(t, page, 1)
		actual = append(actual, page[0].Name)
		token = next
		if next == "" {
			break
		}
	}
	require.Equal(t, []string{"/tree/b", "/tree/c", "tree/b", "tree/c"}, actual)
}

func TestSSMOrderedNamesConcurrentMutationAndDetachedPages(t *testing.T) {
	store := NewSSMStore()
	var group sync.WaitGroup
	failures := make(chan error, 8)
	for worker := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			name := fmt.Sprintf("/owned/w%d", worker)
			for iteration := range 20 {
				if err := store.PutParameter(name, fmt.Sprint(iteration), "String", true); err != nil {
					failures <- err
					return
				}
				page, _, err := store.ListParametersByPath("/owned", true, true, 10, "")
				if err != nil {
					failures <- err
					return
				}
				previous := ""
				for _, parameter := range page {
					if parameter.Name <= previous {
						failures <- fmt.Errorf("page names out of order: %q after %q", parameter.Name, previous)
						return
					}
					previous = parameter.Name
					parameter.Name, parameter.Value = "mutated detached page", "mutated detached page"
				}
				store.DeleteParameterIfExists(name)
			}
			if err := store.PutParameter(name, "final", "String", false); err != nil {
				failures <- err
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	final, token, err := store.ListParametersByPath("/owned", true, true, 10, "")
	require.NoError(t, err)
	require.Empty(t, token)
	require.Len(t, final, 8)
	for _, parameter := range final {
		require.Equal(t, "final", parameter.Value)
		require.Equal(t, int64(1), parameter.Version)
	}
}
