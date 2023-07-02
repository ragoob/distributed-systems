package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type EventSubscription struct {
	Opts *EventSubscriptionOptions
}

func (e *EventSubscription) Subscribe(ctx context.Context) <-chan Event {
	eventChan := make(chan Event)
	conn, err := pgconn.Connect(ctx, e.Opts.ConnStr)
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL server: %v", err)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		log.Fatalf("IdentifySystem failed: %v", err)
	}

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, e.Opts.SlotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalf("CreateReplicationSlot failed: %v", err)
	}
	err = pglogrepl.StartReplication(ctx, conn, e.Opts.SlotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: e.Opts.getPluginArguments()})
	if err != nil {
		log.Fatalf("StartReplication failed: %v", err)
	}

	standbyMessageTimeout := time.Second * 10
	walOpt := &WalOption{
		clientXLogPos:              sysident.XLogPos,
		standbyMessageTimeout:      standbyMessageTimeout,
		nextStandbyMessageDeadline: time.Now().Add(standbyMessageTimeout),
		relations:                  make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:                    pgtype.NewMap(),
		inStream:                   false,
	}

	go e.startWatch(ctx, walOpt, conn, eventChan)

	return eventChan
}

func (e *EventSubscription) startWatch(ctx context.Context, walOpts *WalOption, conn *pgconn.PgConn, eventChan chan Event) {
	defer conn.Close(ctx)
	for {
		if time.Now().After(walOpts.nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: walOpts.clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Printf("Sent Standby status message at %s\n", walOpts.clientXLogPos.String())

			walOpts.nextStandbyMessageDeadline = time.Now().Add(walOpts.standbyMessageTimeout)
		}
		if err := e.receiveAndProcessMessage(conn, walOpts, eventChan); err != nil {
			log.Fatalf("Failed to receive and process message: %v", err)
		}

	}
}

func (e *EventSubscription) receiveAndProcessMessage(conn *pgconn.PgConn, walOpt *WalOption, eventChan chan<- Event) error {
	ctx, cancel := context.WithDeadline(context.Background(), walOpt.nextStandbyMessageDeadline)
	defer cancel()

	rawMsg, err := conn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil
		}
		return fmt.Errorf("ReceiveMessage failed: %v", err)
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		log.Printf("Received unexpected message: %T\n", rawMsg)
		return nil
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		if err := e.processPrimaryKeepaliveMessage(msg.Data[1:], walOpt); err != nil {
			return fmt.Errorf("Failed to process primary keepalive message: %v", err)
		}

	case pglogrepl.XLogDataByteID:
		if err := e.processXLogData(msg.Data[1:], conn, walOpt, eventChan); err != nil {
			return fmt.Errorf("Failed to process XLogData: %v", err)
		}
	}

	return nil
}

func (e *EventSubscription) processPrimaryKeepaliveMessage(data []byte, walOpts *WalOption) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %v", err)
	}

	log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

	if pkm.ReplyRequested {
		walOpts.nextStandbyMessageDeadline = time.Time{}
	}

	return nil
}

func (e *EventSubscription) processXLogData(data []byte, conn *pgconn.PgConn, walOpts *WalOption, eventChan chan<- Event) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("ParseXLogData failed: %v", err)
	}

	process(xld.WALData, walOpts, eventChan)

	walOpts.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
	if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: walOpts.clientXLogPos}); err != nil {
		return fmt.Errorf("SendStandbyStatusUpdate failed: %v", err)
	}

	return nil
}

func process(walData []byte, walOpts *WalOption, eventChan chan<- Event) {
	logicalMsg, err := pglogrepl.ParseV2(walData, walOpts.inStream)
	if err != nil {
		log.Fatalf("Parse logical replication message: %v", err)
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		walOpts.relations[logicalMsg.RelationID] = logicalMsg
	case *pglogrepl.StreamStartMessageV2:
		walOpts.inStream = true
	case *pglogrepl.StreamStopMessageV2:
		walOpts.inStream = false
	case *pglogrepl.InsertMessageV2:
		rel, ok := walOpts.relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}

		fmt.Println(logicalMsg.Tuple.Columns)
		e := Event{
			Data: string(parseTuble(logicalMsg, rel, walOpts)),
		}

		eventChan <- e
	}
}

func parseTuble(logicalMsg *pglogrepl.InsertMessageV2, rel *pglogrepl.RelationMessageV2, opt *WalOption) []byte {
	for idx, col := range logicalMsg.Tuple.Columns {
		colName := rel.Columns[idx].Name

		switch col.DataType {
		case 't':
			val, err := decodeTextColumnData(opt.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				log.Fatalf("error decoding column data: %v", err)
			}
			if colName == "data" {
				d, _ := json.Marshal(val)
				return d
			}
		}
	}
	return nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (e *EventSubscriptionOptions) getPluginArguments() []string {
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", e.PublicationName),
		"messages 'true'",
		"streaming 'true'",
	}

	return pluginArguments
}

type EventSubscriptionOptions struct {
	ConnStr         string
	SlotName        string
	PublicationName string
}

type WalOption struct {
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	relations                  map[uint32]*pglogrepl.RelationMessageV2
	typeMap                    *pgtype.Map
	inStream                   bool
}
