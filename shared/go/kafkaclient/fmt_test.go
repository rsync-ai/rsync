package kafkaclient

import "fmt"

// %v and %+v both route through Stringer; these helpers prove it rather than
// assuming it, since that is what makes an accidental log call safe.
func sprintfV(c Config) string     { return fmt.Sprintf("%v", c) }
func sprintfPlusV(c Config) string { return fmt.Sprintf("%+v", c) }
