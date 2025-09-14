package coordinator

//import (
//	"flag"
//	"fmt"
//	"google.golang.org/grpc"
//	"log"
//	"mapreduce/pkg/coordinator"
//	"mapreduce/pkg/rpc"
//	"net"
//)
//
//func main() {
//	port := flag.Int("port", 8080, "port to listen on")
//	flag.Parse()
//
//	log.Printf("Starting coordinator on port %d\n....", *port)
//
//	master := coordinator.NewMaster()
//	listenAddr := fmt.Sprintf(":%d", *port)
//	listen, err := net.Listen("tcp", listenAddr)
//	if err != nil {
//		log.Fatalf("Failed to listen on port: %d: %v", *port, err)
//	}
//
//	s := grpc.NewServer()
//	rpc.RegisterCoordinatorServer(s, master)
//
//	log.Printf("Coordinator server listening at %v", listen.Addr())
//	if err := s.Serve(listen); err != nil {
//		log.Fatalf("Failed to serve: %v", err)
//	}
//}
