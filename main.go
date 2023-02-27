package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/jackc/pglogrepl"
    "github.com/jackc/pgx/v5/pgconn"
    "github.com/jackc/pgx/v5/pgproto3"
    "github.com/jackc/pgx/v5/pgtype"
)

const (
    outputPlugin = "pgoutput"
    slotName     = "pglogrepl_demo"

    createTable       = "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name TEXT, toast TEXT);"
    createPublication = "CREATE PUBLICATION %s FOR ALL TABLES;"
)

var (
    typeMap   = pgtype.NewMap()
    relations = map[uint32]*pglogrepl.RelationMessage{}
)

func main() {
    conn, err := pgconn.Connect(context.Background(), "postgres://postgres@127.0.0.1/postgres?replication=database")
    if err != nil {
        log.Fatalf("failed to connect to PostgreSQL server: %v", err)
    }
    defer conn.Close(context.Background())

    if _, err := conn.Exec(context.Background(), createTable).ReadAll(); err != nil {
        log.Fatalf("failed to create table: %v", err)
    }

    if _, err := conn.Exec(context.Background(), fmt.Sprintf(createPublication, slotName)).ReadAll(); err != nil {
        if pge, ok := err.(*pgconn.PgError); !ok || pge.Code != "42710" {
            log.Fatalf("failed to create publication: %v", err)
        }
    }

    ident, err := pglogrepl.IdentifySystem(context.Background(), conn)
    if err != nil {
        log.Fatalf("failed to identify system: %v", err)
    }
    log.Printf("SystemID: %s Timeline: %d XLogPos: %d DBName: %s\n", ident.SystemID, ident.Timeline, ident.XLogPos, ident.DBName)

    if _, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{}); err != nil {
        if pge, ok := err.(*pgconn.PgError); !ok || pge.Code != "42710" {
            log.Fatalf("failed to create logical replication slot: %v", err)
        }
    }
    log.Println("Created temporary replication slot:", slotName)

    if err = pglogrepl.StartReplication(context.Background(), conn, slotName, ident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", slotName), "binary 'true'"}}); err != nil {
        log.Fatalf("failed to start replication: %v", err)
    }
    log.Println("Logical replication started on slot", slotName)

    clientXLogPos := ident.XLogPos
    standbyMessageTimeout := time.Second * 10
    nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

    for {
        if time.Now().After(nextStandbyMessageDeadline) {
            if err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos}); err != nil {
                log.Fatalf("failed to send standby status update: %v", err)
            }
            log.Println("sent standby status update")
            nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
        }

        ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
        msg, err := conn.ReceiveMessage(ctx)
        cancel()
        if err != nil {
            if pgconn.Timeout(err) {
                log.Printf("receive message but timeout: %v\n", err)
                continue
            }
            log.Fatalf("failed to receive message: %v", err)
        }

        switch msg := msg.(type) {
        case *pgproto3.CopyData:
            switch msg.Data[0] {
            case pglogrepl.PrimaryKeepaliveMessageByteID:
                var pkm pglogrepl.PrimaryKeepaliveMessage
                if pkm, err = pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:]); err == nil && pkm.ReplyRequested {
                    nextStandbyMessageDeadline = time.Time{}
                }
                log.Printf("primary keepalive message: ServerWaLEnd: %d ServerTime: %v ReplyRequested: %v\n", pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested)
            case pglogrepl.XLogDataByteID:
                xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
                if err != nil {
                    log.Fatalf("failed to parse xlog data: %v", err)
                }
                log.Printf("xlog data: WALStart %s ServerWALEnd %s ServerTime %s\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime)

                logicalMsg, err := pglogrepl.Parse(xld.WALData)
                if err != nil {
                    log.Fatalf("failed to parse logical replication message: %v", err)
                }

                switch logicalMsg := logicalMsg.(type) {
                case *pglogrepl.RelationMessage:
                    relations[logicalMsg.RelationID] = logicalMsg
                    log.Println("receive relation message")
                case *pglogrepl.BeginMessage:
                    log.Printf("receive begin message: xid: %d finalLSN: %d commitTime: %v\n", logicalMsg.Xid, logicalMsg.FinalLSN, logicalMsg.CommitTime)
                case *pglogrepl.CommitMessage:
                    log.Printf("receive commit message: commitLSN: %d transactionEndLSN: %d commitTime: %v\n", logicalMsg.CommitLSN, logicalMsg.TransactionEndLSN, logicalMsg.CommitTime)
                case *pglogrepl.InsertMessage:
                    rel, ok := relations[logicalMsg.RelationID]
                    if !ok {
                        log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
                    }
                    values := getValues(rel, logicalMsg.Tuple)
                    log.Printf("receive insert message: newValues: %v\n", values)
                case *pglogrepl.UpdateMessage:
                    rel, ok := relations[logicalMsg.RelationID]
                    if !ok {
                        log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
                    }

                    // oldValues will only have values when the Replica Identity of the Table is set to full mode.
                    oldValues := getValues(rel, logicalMsg.OldTuple)
                    // toast columns will only have values when the replica identity is set to full mode or update toast columns
                    newValues := getValues(rel, logicalMsg.NewTuple)
                    log.Printf("receive update message: oldValues: %v newValues: %v\n", oldValues, newValues)
                case *pglogrepl.DeleteMessage:
                    rel, ok := relations[logicalMsg.RelationID]
                    if !ok {
                        log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
                    }

                    // oldValues except primary key column will only have values when the Replica Identity of the Table is set to full mode.
                    oldValues := getValues(rel, logicalMsg.OldTuple)
                    log.Printf("receive delete message: oldValues: %v\n", oldValues)
                case *pglogrepl.TruncateMessage:
                    log.Println("receive truncate message")
                case *pglogrepl.TypeMessage:
                    log.Println("receive type message")
                case *pglogrepl.OriginMessage:
                    log.Println("receive origin message")
                default:
                    log.Printf("receive unknown message type in pgoutput stream: %v\n", logicalMsg)
                }

                clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
                log.Printf("clientXLogPos: %d\n", clientXLogPos)
            }
        default:
            log.Fatalf("receive unexpcted message: %v", msg)
        }
    }
}

func getValues(rel *pglogrepl.RelationMessage, tupleData *pglogrepl.TupleData) map[string]interface{} {
    if tupleData == nil {
        return nil
    }

    values := make(map[string]interface{}, len(tupleData.Columns))
    for idx, col := range tupleData.Columns {
        colName := rel.Columns[idx].Name
        switch col.DataType {
        case 'n': // null
            values[colName] = nil
        case 'u': // unchanged toast
        case 't': // text
            val, err := decodeData(typeMap, col.Data, rel.Columns[idx].DataType, pgtype.TextFormatCode)
            if err != nil {
                log.Fatalf("faield to decode text column data: %v", err)
            }
            values[colName] = val
        case 'b': // binary
            val, err := decodeData(typeMap, col.Data, rel.Columns[idx].DataType, pgtype.BinaryFormatCode)
            if err != nil {
                log.Fatalf("faield to decode binary column data: %v", err)
            }
            values[colName] = val
        }
    }
    return values
}

func decodeData(mi *pgtype.Map, data []byte, dataType uint32, format int16) (interface{}, error) {
    if dt, ok := mi.TypeForOID(dataType); ok {
        return dt.Codec.DecodeValue(mi, dataType, format, data)
    }
    return string(data), nil
}
