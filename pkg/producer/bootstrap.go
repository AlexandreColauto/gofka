package producer

import (
	pb "github.com/alexandrecolauto/gofka/proto/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"strings"
)

type Bootstrap struct {
	address    string
	connection *grpc.ClientConn
	client     pb.ProducerServiceClient
}

func (p *Producer) ConnectToBootstrap() error {
	conn, err := grpc.NewClient(p.bootstrap.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}

	cli := pb.NewProducerServiceClient(conn)
	p.bootstrap.client = cli
	p.bootstrap.connection = conn
	return nil
}

func (s *Producer) checkErrAndRedirect(err error, fun func() error) error {
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.FailedPrecondition {
			// Parse the error message for leader info
			parts := strings.Split(st.Message(), "|")
			if len(parts) == 3 && parts[0] == "not leader" {
				// leaderID := parts[1]
				leaderAddr := parts[2]

				// log.Printf("Redirecting to leader %s at %s", leaderID, leaderAddr)
				s.updateAddress(leaderAddr)
				return fun()
			}
		}
	}
	return err
}

func (p *Producer) updateAddress(address string) {
	p.bootstrap.address = address
}
