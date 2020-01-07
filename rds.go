package AwsRds

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

var sess *session.Session
var rdsClient *rds.RDS
var Databases DBS

type DBS struct {
	RegisteredDbMap    map[string]*gorm.DB
	RegisteredDbDsnMap map[string]string
}

func init() {

	if Databases.RegisteredDbMap == nil {
		Databases.RegisteredDbMap = make(map[string]*gorm.DB)
	}

	if Databases.RegisteredDbDsnMap == nil {
		Databases.RegisteredDbDsnMap = make(map[string]string)
	}

	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	rdsClient = rds.New(sess)
}

func GormDB(clusterName, database string, write bool) *gorm.DB {
	if write {
		db, ok := Databases.RegisteredDbMap[fmt.Sprintf("%s-%s__writer", clusterName, database)]
		if ok {
			err := db.DB().Ping()
			if err != nil {
				log.Println("[error] write DB connection closed, attempting reopen...", err)
				db, err = createConnectionFromDsn(Databases.RegisteredDbDsnMap[fmt.Sprintf("%s-%s__writer", clusterName, database)])
			}

			if db.DB().Stats().OpenConnections == db.DB().Stats().MaxOpenConnections {
				err := errors.New("Too many connections open")
				log.Println("[error] Too many connections open on the write DB", err, db.DB().Stats())
			}

			return db
		}
	} else {
		dbReader, ok := Databases.RegisteredDbMap[fmt.Sprintf("%s-%s__reader", clusterName, database)]

		if ok {
			err := dbReader.DB().Ping()
			if err != nil {
				log.Println("[error] read DB connection closed, attempting reopen...", err)
				dbReader, err = createConnectionFromDsn(Databases.RegisteredDbDsnMap[fmt.Sprintf("%s-%s__writer", clusterName, database)])
			}

			if dbReader.DB().Stats().OpenConnections == dbReader.DB().Stats().MaxOpenConnections {
				err := errors.New("Too many connections open")
				log.Println("[error] Too many connections open on the read DB", err, dbReader.DB().Stats())
			}

			return dbReader
		}
	}

	return nil
}

func DB(clusterName, database string, write bool) *sql.DB {
	return GormDB(clusterName, database, write).DB()
}

func Query(clusterName, database, query string, args ...interface{}) (rows *sql.Rows, err error) {
	if isWriteRequred(query) {
		if db, ok := Databases.RegisteredDbMap[fmt.Sprintf("%s-%s__writer", clusterName, database)]; ok {
			rows, err = db.DB().Query(query, args...)

			if err != nil {
				log.Println("[error] Failed to attempt write query on DB", query, db.DB().Stats())

				if db.DB().Stats().OpenConnections == db.DB().Stats().MaxOpenConnections {
					err = errors.New("Too many connections open")
					log.Println("[error] Too many connections open on the write DB", err, db.DB().Stats())
					return
				}

				err = db.DB().Ping()
				if err != nil {
					log.Println("[error] write DB connection closed, attempting reopen...", err)
					db, err = createConnectionFromDsn(Databases.RegisteredDbDsnMap[fmt.Sprintf("%s-%s__writer", clusterName, database)])

					if err != nil {
						log.Println("[error] Failed to restablish connection", err)
						return
					}

					rows, err = db.DB().Query(query, args...)
				}
			}
		}
	} else {
		if db, ok := Databases.RegisteredDbMap[fmt.Sprintf("%s-%s__reader", clusterName, database)]; ok {
			rows, err = db.DB().Query(query, args...)

			if err != nil {
				log.Println("[error] Failed to attempt read query on DB", query, db.DB().Stats())

				if db.DB().Stats().OpenConnections == db.DB().Stats().MaxOpenConnections {
					err = errors.New("Too many connections open")
					log.Println("[error] Too many connections open on the read DB", err, db.DB().Stats())
					return
				}

				err = db.DB().Ping()
				if err != nil {
					log.Println("[error] read DB connection closed, attempting reopen...", err)
					db, err = createConnectionFromDsn(Databases.RegisteredDbDsnMap[fmt.Sprintf("%s-%s__reader", clusterName, database)])
					if err != nil {
						log.Println("[error] Failed to restablish connection", err)
						return
					}

					rows, err = db.DB().Query(query, args...)
				}
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

func RegisterCluster(clusterName, database, username, password string) (err error) {
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

			db, dsn, err := createConnection(*instance.Endpoint, database, username, password)

			if err != nil {
				log.Println("Failed to create connection to writer DB", err)
				continue
			}

			if strings.ToLower(*instance.EndpointType) == "writer" {
				mapKey := fmt.Sprintf("%s-%s__writer", clusterName, database)
				Databases.RegisteredDbMap[mapKey] = db
				Databases.RegisteredDbDsnMap[mapKey] = dsn
			} else {
				mapKey := fmt.Sprintf("%s-%s__reader", clusterName, database)
				Databases.RegisteredDbMap[mapKey] = db
				Databases.RegisteredDbDsnMap[mapKey] = dsn
			}
		}
	}

	return
}

func createConnection(endpoint, database, username, password string) (db *gorm.DB, dsn string, err error) {
	dsn = createDsn(endpoint, database, username, password)
	db, err = gorm.Open("mysql", dsn)
	return
}

func createConnectionFromDsn(dsn string) (db *gorm.DB, err error) {
	db, err = gorm.Open("mysql", dsn)
	return
}

func createDsn(endpoint, database, username, password string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?collation=utf8_general_ci&parseTime=true", username, password, endpoint, database)
}
