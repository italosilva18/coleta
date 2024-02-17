package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	connectionString := "mongodb+srv://suporte:Italo2013@suporte.ifkalhd.mongodb.net/?retryWrites=true&w=majority"
	client, err := mongo.NewClient(options.Client().ApplyURI(connectionString))
	if err != nil {
		fmt.Println("Erro ao criar o cliente MongoDB:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		fmt.Println("Erro ao conectar ao MongoDB:", err)
		return
	}
	defer client.Disconnect(ctx)

	fmt.Println("Conectado ao MongoDB com sucesso")

	// Teste de Inserção
	collection := client.Database("Suporte").Collection("LOJAS")
	result, err := collection.InsertOne(ctx, bson.M{"chave": "valor"})
	if err != nil {
		fmt.Println("Erro ao inserir documento:", err)
		return
	}

	fmt.Println("Documento inserido com ID:", result.InsertedID)
}
