package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// Configuração do MySQL Server
	mysqlConfig := mysql.NewConfig()
	mysqlConfig.User = "root"
	mysqlConfig.Passwd = "italo2013"
	mysqlConfig.Net = "tcp"
	mysqlConfig.Addr = "localhost:3306"
	mysqlConfig.DBName = "testgo"

	// Configuração do MongoDB
	mongoURI := "mongodb+srv://suporte:Italo2013@suporte.ifkalhd.mongodb.net/?retryWrites=true&w=majority"
	mongoDBName := "Suporte"
	mongoCollectionName := "LOJAS"

	// Conexão com o MySQL Server
	mysqlDB, err := sql.Open("mysql", mysqlConfig.FormatDSN())
	if err != nil {
		log.Fatal("Erro ao conectar ao MySQL:", err)
	}
	defer mysqlDB.Close()

	// Recupera a lista de tabelas no banco de dados
	tables, err := getTables(mysqlDB)
	if err != nil {
		log.Fatal("Erro ao recuperar as tabelas:", err)
	}

	// Conexão com o MongoDB
	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("Erro ao conectar ao MongoDB:", err)
	}
	defer mongoClient.Disconnect(context.Background())

	mongoDB := mongoClient.Database(mongoDBName)

	// Para cada tabela, recupera os dados e os envia para o MongoDB
	for _, table := range tables {
		data, err := getTableData(mysqlDB, table)
		if err != nil {
			log.Printf("Erro ao recuperar os dados da tabela %s: %v\n", table, err)
			continue
		}

		err = insertData(mongoDB, mongoCollectionName, data)
		if err != nil {
			log.Printf("Erro ao enviar os dados da tabela %s para o MongoDB: %v\n", table, err)
			continue
		}

		fmt.Printf("Dados da tabela %s enviados com sucesso para o MongoDB!\n", table)
	}
}

// Recupera a lista de tabelas no banco de dados
func getTables(db *sql.DB) ([]string, error) {
	var tables []string

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// Recupera os dados de uma tabela específica
func getTableData(db *sql.DB, table string) ([]map[string]interface{}, error) {
	var data []map[string]interface{}

	query := fmt.Sprintf("SELECT * FROM %s", table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		err := rows.Scan(pointers...)
		if err != nil {
			return nil, err
		}

		entry := make(map[string]interface{})
		for i, column := range columns {
			value := values[i]
			switch v := value.(type) {
			case nil:
				entry[column] = nil
			case []byte:
				entry[column] = string(v)
			default:
				entry[column] = v
			}
		}

		data = append(data, entry)
	}

	return data, nil
}

// Insere os dados no MongoDB
func insertData(db *mongo.Database, collection string, data []map[string]interface{}) error {
	mongoCollection := db.Collection(collection)

	docs := make([]interface{}, len(data))
	for i, d := range data {
		docs[i] = d
	}

	_, err := mongoCollection.InsertMany(context.Background(), docs)
	if err != nil {
		return err
	}

	return nil
}
