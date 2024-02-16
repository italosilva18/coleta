package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config representa a estrutura do arquivo de configuração
type Config struct {
	LocalDBConnection string            `json:"localDBConnection"`
	MongoDBConnection string            `json:"mongoDBConnection"`
	Queries           map[string]string `json:"queries"`
}

func main() {
	// Carrega as configurações do arquivo JSON
	config, err := loadConfig("coleta/config.json")
	if err != nil {
		log.Fatal("Erro ao carregar as configurações:", err)
	}

	// Conecta ao banco local
	localDB, err := connectToLocalDB(config.LocalDBConnection)
	if err != nil {
		log.Fatal("Erro ao conectar ao banco local:", err)
	}
	defer localDB.Disconnect(context.Background())

	// Acesse as consultas
	for nomeConsulta, query := range config.Queries {
		fmt.Printf("Executando a consulta %s: %s\n", nomeConsulta, query)

		// Implemente a lógica de execução da consulta aqui
		dados, err := executeQuery(localDB, query)
		if err != nil {
			log.Printf("Erro ao executar a consulta %s: %v", nomeConsulta, err)
			continue
		}

		// Conecta ao MongoDB
		mongoClient, err := connectToMongoDB(config.MongoDBConnection)
		if err != nil {
			log.Fatal("Erro ao conectar ao MongoDB:", err)
		}
		defer mongoClient.Disconnect(context.Background())

		// Envia dados para o MongoDB
		err = sendDataToMongoDB(mongoClient, dados)
		if err != nil {
			log.Printf("Erro ao enviar dados para o MongoDB após a consulta %s: %v", nomeConsulta, err)
		} else {
			fmt.Printf("Dados da consulta %s enviados para o MongoDB com sucesso!\n", nomeConsulta)
		}
	}
}

func loadConfig(filePath string) (*Config, error) {
	// Lê o arquivo de configuração
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// Parse do JSON para a estrutura Config
	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func connectToLocalDB(connection string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(connection))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func executeQuery(client *mongo.Client, query string) ([]interface{}, error) {
	// Implemente a lógica de execução da consulta no banco local
	// Substitua o código abaixo com a sua lógica real
	collection := client.Database("seu_banco").Collection("sua_colecao")
	cursor, err := collection.Find(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var result []interface{}
	for cursor.Next(context.Background()) {
		var data interface{}
		err := cursor.Decode(&data)
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}

	return result, nil
}

func connectToMongoDB(connection string) (*mongo.Client, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(connection))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func sendDataToMongoDB(client *mongo.Client, data []interface{}) error {
	// Implemente a lógica de envio de dados para o MongoDB
	// Substitua o código abaixo com a sua lógica real
	collection := client.Database("seu_banco_destino").Collection("sua_colecao_destino")

	for _, d := range data {
		_, err := collection.InsertOne(context.Background(), d)
		if err != nil {
			return err
		}
	}

	return nil
}
