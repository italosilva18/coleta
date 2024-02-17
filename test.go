package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
)

func test() {
	// Substitua as informações de conexão com as configurações reais
	server := "localhost"
	port := 1433
	user := "seu_usuario"
	password := "sua_senha"
	database := "seu_banco_de_dados"

	// Construa a string de conexão
	connectionString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", server, user, password, port, database)

	// Tenta conectar ao SQL Server
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		log.Fatal("Erro ao conectar ao SQL Server:", err)
	}
	defer db.Close()

	// Tenta fazer um ping para verificar a conexão
	err = db.Ping()
	if err != nil {
		log.Fatal("Erro ao fazer ping no SQL Server:", err)
	}

	log.Println("Conexão com o SQL Server bem-sucedida")
}
