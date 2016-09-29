/**
# *　　　　 ┏┓　　 　┏┓+ +
# *　　　　┏┛┻━━━━━━━┛┻━━┓　 + +
# *　　　　┃　　　　　　 ┃
# *　　　　┃━　　━　　 　┃ ++ + + +
# *　　　 ████━████      ┃+
# *　　　　┃　　　　　　 ┃ +
# *　　　　┃　┻　　　    ┃
# *　　　　┃　　　　　　 ┃ + +
# *　　　　┗━━━┓　　 　┏━┛
# *　　　　　　┃　　 　┃
# *　　　　　　┃　　 　┃ + + + +
# *　　　　　　┃　　 　┃　　　Code is far away from bug
# *　　　　　　┃　　 　┃　　　with the animal protecting
# *　　　　　　┃　　 　┃
# *　　　　　　┃　　 　┃ + 　　　神兽保佑,代码无bug
# *　　　　　　┃　　 　┃
# *　　　　　　┃　　 　┃　　+
# *　　　　　　┃　 　　┗━━━━━━━┓ + +
# *　　　　　　┃ 　　　　　　　┣┓
# *　　　　　　┃ 　　　　　　　┏┛
# *　　　　　　┗━━┓┓┏━━━━━┳┓┏━━┛ + + + +
# *　　　　　　　 ┃┫┫　   ┃┫┫
# *　　　　　　　 ┗┻┛　   ┗┻┛+ + + +
# */

package haystack

import (
	"fmt"
	"testing"
)

func TestData(t *testing.T) {
	np := NeedlePosition{1, 32}
	value := np.to_int64()
	fmt.Println("np.to_int64() value=", value)

	np0 := NeedlePosition{}
	np0.from_int64(value)
	if np0.Offset != 1 || np0.Size != 32 {
		t.Errorf("NeedlePosition.from_int64() failed. np0.Offset=%d np0.Size=%d", np0.Offset, np0.Size)
	}
}
