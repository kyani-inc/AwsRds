package AwsRds

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"log"
	"strings"
)

var sess *session.Session
var rdsClient *rds.RDS
var RegisteredDbMap map[string]*sql.DB
var RegisteredDbDsnMap map[string]string

func init() {

	if RegisteredDbMap == nil {
		RegisteredDbMap = make(map[string]*sql.DB)
	}

	if RegisteredDbDsnMap == nil {
		RegisteredDbDsnMap = make(map[string]string)
	}

	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	rdsClient = rds.New(sess)
}

func Query(clusterName, query string, args ...interface{}) (rows *sql.Rows, err error) {
	if isWriteRequred(query) {
		db := RegisteredDbMap[fmt.Sprintf("%s__writer", clusterName)]
		rows, err = db.Query(query, args)

		if err != nil {
			log.Println("[error] Failed to attempt write query on DB", query, db.Stats())

			if db.Stats().OpenConnections == db.Stats().MaxOpenConnections {
				err = errors.New("Too many connections open")
				log.Println("[error] Too many connections open on the write DB", err, db.Stats())
				return
			}

			err = db.Ping()
			if err != nil {
				log.Println("[error] write DB connection closed, attempting reopen...", err)
				db, err = createConnectionFromDsn(RegisteredDbDsnMap[fmt.Sprintf("%s__writer", clusterName)])

				if err != nil {
					log.Println("[error] Failed to restablish connection", err)
					return
				}

				rows, err = db.Query(query, args)
			}
		}
	} else {
		db := RegisteredDbMap[fmt.Sprintf("%s__reader", clusterName)]
		rows, err = db.Query(query, args)

		if err != nil {
			log.Println("[error] Failed to attempt read query on DB", query, db.Stats())

			if db.Stats().OpenConnections == db.Stats().MaxOpenConnections {
				err = errors.New("Too many connections open")
				log.Println("[error] Too many connections open on the read DB", err, db.Stats())
				return
			}

			err = db.Ping()
			if err != nil {
				log.Println("[error] read DB connection closed, attempting reopen...", err)
				db, err = createConnectionFromDsn(RegisteredDbDsnMap[fmt.Sprintf("%s__writer", clusterName)])
				if err != nil {
					log.Println("[error] Failed to restablish connection", err)
					return
				}

				rows, err = db.Query(query, args)
			}
		}
	}
	return
}

func isWriteRequred(query string) (isRequired bool) {
	queryLowercase := strings.ToLower(query)
	if strings.Contains(queryLowercase, "insert") {
		isRequired = true
	}
	if strings.Contains(queryLowercase, "update") {
		isRequired = true
	}
	return
}

func RegisterCluster(clusterName, username, password string) (err error) {
	input := rds.DescribeDBClusterEndpointsInput{
		DBClusterEndpointIdentifier: nil,
		DBClusterIdentifier:         aws.String(clusterName),
		Filters:                     nil,
		Marker:                      nil,
		MaxRecords:                  nil,
	}

	out, err := rdsClient.DescribeDBClusterEndpoints(&input)

	if err != nil {
		return
	}

	list := out.DBClusterEndpoints

	for _, instance := range list {
		if strings.ToLower(*instance.Status) == "available" {

			db, dsn, err := createConnection(*instance.Endpoint, username, password)

			if err != nil {
				log.Println("Failed to create connection to writer DB", err)
				continue
			}

			if strings.ToLower(*instance.EndpointType) == "writer" {
				mapKey := fmt.Sprintf("%s__writer", clusterName)
				RegisteredDbMap[mapKey] = db
				RegisteredDbDsnMap[mapKey] = dsn
			} else {
				mapKey := fmt.Sprintf("%s__reader", clusterName)
				RegisteredDbMap[mapKey] = db
				RegisteredDbDsnMap[mapKey] = dsn
			}
		}
	}

	return
}

func createConnection(endpoint, username, password string) (db *sql.DB, dsn string, err error) {
	dsn = createDsn(endpoint, username, password)
	db, err = sql.Open("mysql", dsn)
	return
}

func createConnectionFromDsn(dsn string) (db *sql.DB, err error) {
	db, err = sql.Open("mysql", dsn)
	return
}

func createDsn(endpoint, username, password string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)", username, password, endpoint)
}
