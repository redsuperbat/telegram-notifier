package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	"rsb.asuscomm.com/telegram-notifier/consuming"
)

type ChatCreatedEvent struct {
	ChatId string `json:"chat_id"`
}

func loadEnvVars() {
	log.Println("Loading env variables")
	if _, err := os.Stat(".env"); errors.Is(err, os.ErrNotExist) {
		log.Println("No .env file present skipping parsing it.")
		return
	}
	if err := godotenv.Load(".env"); err != nil {
		log.Fatalf("Error loading .env file %v", err.Error())
	}
}

func main() {
	loadEnvVars()
	botToken := os.Getenv("TELEGRAM_BOT_KEY")
	if botToken == "" {
		log.Fatalln("No telegram bot token provided")
	}
	chatIdStr := os.Getenv("TELEGRAM_CHAT_ID")
	if chatIdStr == "" {
		log.Fatalln("No chat id was provided")
	}
	chatId, err := strconv.Atoi(chatIdStr)
	if err != nil {
		log.Fatalln("Invalid chat id provided, expected number got", chatIdStr)
	}
	msgChan := make(chan []byte)
	startConsuming, stopConsuming := consuming.NewConsumer(msgChan)
	go startConsuming()
	defer stopConsuming()

	// Telegram stuff
	bot, err := tgbotapi.NewBotAPI(botToken)

	if err != nil {
		log.Panicf("Unable to create telegram bot API because of %v", err.Error())
	}

	// Listen to the stream and send a message to telegram if someone wants to start a chat
	for kafkaMsg := range msgChan {
		var event ChatCreatedEvent
		if err := json.Unmarshal(kafkaMsg, &event); err != nil {
			log.Println("Unable to Unmarshal event because of error", err.Error())
			continue
		}
		url := "https://chat.netterberg.me/chats/" + event.ChatId
		text := fmt.Sprintf("Hey, someone wants to chat with you!\nVisit %s to chat with them!", url)
		msg := tgbotapi.NewMessage(int64(chatId), text)
		log.Printf("New chat was started sending %s to chat %v", text, chatId)
		if _, err := bot.Send(msg); err != nil {
			log.Printf("Unable to send message because of %v", err.Error())
		}

	}

}
