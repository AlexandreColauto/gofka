package topic

import (
	"os"
	"testing"
	"time"

	"github.com/alexandrecolauto/gofka/common/proto/broker"
	"github.com/stretchr/testify/assert"
)

func TestTopic(t *testing.T) {
	path := "test"

	t.Log("Hello world , working")
	l, err := NewLog(path)
	defer clean(t, l)
	assert.NoError(t, err)

	msgs := msgs()

	l.AppendBatch(msgs)
	time.Sleep(1 * time.Second)
	l.AppendBatch(msgs)
	time.Sleep(1 * time.Second)
	l.AppendBatch(msgs)
	time.Sleep(1 * time.Second)
	l.AppendBatch(msgs)
	time.Sleep(1 * time.Second)
	l.active.Close()
	nl, err := NewLog(path)
	t.Log(nl.active)
	time.Sleep(1 * time.Second)
	assert.True(t, false)
}

func msgs() []*broker.Message {
	res := []*broker.Message{}
	val := `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam molestie vestibulum dolor, tempor fermentum ligula elementum ac. Suspendisse potenti. Nunc luctus maximus sodales. Praesent bibendum libero id dolor elementum, nec porttitor mi suscipit. Duis mollis nibh eget risus accumsan consectetur. Donec blandit risus vitae ex facilisis, quis pharetra risus tempor. Fusce eget sodales justo. Sed eu risus cursus nisi volutpat hendrerit.
Quisque sem dolor, ultrices eget elementum eu, iaculis sagittis nibh. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam malesuada dolor et feugiat pretium. Vestibulum finibus leo quis ipsum feugiat, a efficitur mi malesuada. Donec volutpat dui a mattis auctor. Duis id rutrum felis, rhoncus venenatis purus. Nullam lobortis fermentum odio, non porta felis bibendum eu. Nulla vestibulum, dolor nec vehicula vehicula, est purus rhoncus risus, ut pharetra eros urna at nisl. Aliquam erat volutpat. Cras maximus imperdiet ante non mollis.
Donec non convallis sapien. Aliquam nec neque id magna eleifend bibendum quis vel ligula. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Phasellus ac neque vestibulum, ultricies enim non, venenatis tellus. Cras faucibus porttitor felis, sed tristique nibh egestas id. Ut pretium elementum dapibus. Maecenas odio est, facilisis quis eros nec, blandit finibus elit. Vestibulum tristique vel lacus vitae tristique. Maecenas sed odio leo. Nullam volutpat sem ut tincidunt gravida. Phasellus non diam id dui pretium ornare non nec tortor.
Vivamus in vestibulum quam. Nulla purus dolor, interdum at rutrum in, sollicitudin non urna. Phasellus vitae ligula eu arcu varius scelerisque. Praesent posuere finibus tortor nec blandit. In sollicitudin ultricies eros vel ultrices. Vivamus vitae ultrices ligula. Phasellus et vulputate urna. Praesent sit amet ligula sed velit consectetur porttitor. Aliquam et nunc risus. Etiam at sodales tellus. Ut laoreet, sem ac eleifend eleifend, sem ipsum pretium justo, eu dictum lacus lorem ac massa.
Quisque sem dolor, ultrices eget elementum eu, iaculis sagittis nibh. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam malesuada dolor et feugiat pretium. Vestibulum finibus leo quis ipsum feugiat, a efficitur mi malesuada. Donec volutpat dui a mattis auctor. Duis id rutrum felis, rhoncus venenatis purus. Nullam lobortis fermentum odio, non porta felis bibendum eu. Nulla vestibulum, dolor nec vehicula vehicula, est purus rhoncus risus, ut pharetra eros urna at nisl. Aliquam erat volutpat. Cras maximus imperdiet ante non mollis.
Donec non convallis sapien. Aliquam nec neque id magna eleifend bibendum quis vel ligula. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Phasellus ac neque vestibulum, ultricies enim non, venenatis tellus. Cras faucibus porttitor felis, sed tristique nibh egestas id. Ut pretium elementum dapibus. Maecenas odio est, facilisis quis eros nec, blandit finibus elit. Vestibulum tristique vel lacus vitae tristique. Maecenas sed odio leo. Nullam volutpat sem ut tincidunt gravida. Phasellus non diam id dui pretium ornare non nec tortor.
Vivamus in vestibulum quam. Nulla purus dolor, interdum at rutrum in, sollicitudin non urna. Phasellus vitae ligula eu arcu varius scelerisque. Praesent posuere finibus tortor nec blandit. In sollicitudin ultricies eros vel ultrices. Vivamus vitae ultrices ligula. Phasellus et vulputate urna. Praesent sit amet ligula sed velit consectetur porttitor. Aliquam et nunc risus. Etiam at sodales tellus. Ut laoreet, sem ac eleifend eleifend, sem ipsum pretium justo, eu dictum lacus lorem ac massa.
Quisque sem dolor, ultrices eget elementum eu, iaculis sagittis nibh. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam malesuada dolor et feugiat pretium. Vestibulum finibus leo quis ipsum feugiat, a efficitur mi malesuada. Donec volutpat dui a mattis auctor. Duis id rutrum felis, rhoncus venenatis purus. Nullam lobortis fermentum odio, non porta felis bibendum eu. Nulla vestibulum, dolor nec vehicula vehicula, est purus rhoncus risus, ut pharetra eros urna at nisl. Aliquam erat volutpat. Cras maximus imperdiet ante non mollis.
Donec non convallis sapien. Aliquam nec neque id magna eleifend bibendum quis vel ligula. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Phasellus ac neque vestibulum, ultricies enim non, venenatis tellus. Cras faucibus porttitor felis, sed tristique nibh egestas id. Ut pretium elementum dapibus. Maecenas odio est, facilisis quis eros nec, blandit finibus elit. Vestibulum tristique vel lacus vitae tristique. Maecenas sed odio leo. Nullam volutpat sem ut tincidunt gravida. Phasellus non diam id dui pretium ornare non nec tortor.
Vivamus in vestibulum quam. Nulla purus dolor, interdum at rutrum in, sollicitudin non urna. Phasellus vitae ligula eu arcu varius scelerisque. Praesent posuere finibus tortor nec blandit. In sollicitudin ultricies eros vel ultrices. Vivamus vitae ultrices ligula. Phasellus et vulputate urna. Praesent sit amet ligula sed velit consectetur porttitor. Aliquam et nunc risus. Etiam at sodales tellus. Ut laoreet, sem ac eleifend eleifend, sem ipsum pretium justo, eu dictum lacus lorem ac massa.
Donec mattis magna id turpis malesuada, quis scelerisque purus blandit. Suspendisse eleifend mollis scelerisque. Donec at tortor tellus. Aliquam erat volutpat. Fusce eleifend, ex quis imperdiet tristique, ipsum est viverra urna, vel euismod nisi magna eget orci. Mauris eget augue vitae dui sagittis euismod eget non lectus. Cras varius ligula et enim faucibus, ac mattis augue ornare`
	val = "msg value - msg value - msg value - msg value - msg value - msg value - msg value - msg value - msg value - msg value - msg value - msg value - msg value - msg value"
	for range 50 {
		msg := &broker.Message{
			Value:     val,
			Topic:     "foo-topic",
			Partition: 0,
		}
		res = append(res, msg)
	}
	return res
}

func clean(t *testing.T, l *Log) {
	t.Log("Cleaning log")
	err := os.RemoveAll(l.dir)
	assert.NoError(t, err)
}
