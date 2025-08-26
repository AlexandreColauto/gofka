package producer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Producer struct {
	grpcConn      *grpc.ClientConn
	grpcClient    pb.ProducerServiceClient
	topic         string
	brokerAddress string
}

func NewProducer(topic string, brokerAddress string) *Producer {
	return &Producer{topic: topic, brokerAddress: brokerAddress}
}

func (p *Producer) ConnectToBroker() {
	conn, err := grpc.NewClient(p.brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}

	cli := pb.NewProducerServiceClient(conn)
	p.grpcClient = cli
}

func (p *Producer) SendMessage(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &pb.SendMessageRequest{
		Topic: p.topic,
		Key:   key,
		Value: value,
	}
	res, err := p.grpcClient.HandleSendMessage(ctx, req)
	if err != nil {
		fmt.Println("client - send msg erro: ", err)
		fn := func() error { return p.SendMessage(key, value) }
		return p.checkErrAndRedirect(err, fn)
	}
	if !res.Success {
		return fmt.Errorf(res.ErrorMsg)
	}
	fmt.Println("Message sent")
	return nil
}

func (s *Producer) checkErrAndRedirect(err error, fun func() error) error {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.FailedPrecondition {
			// Parse the error message for leader info
			parts := strings.Split(st.Message(), "|")
			if len(parts) == 3 && parts[0] == "not leader" {
				leaderID := parts[1]
				leaderAddr := parts[2]

				log.Printf("Redirecting to leader %s at %s", leaderID, leaderAddr)
				s.updateAddress(leaderAddr)
				return fun()
			}
		}
	}
	return err
}
func (p *Producer) updateAddress(address string) {
	p.brokerAddress = address
}
