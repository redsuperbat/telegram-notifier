package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/joho/godotenv"
	"rsb.asuscomm.com/telegram-notifier/consuming"
)

type ChatStartedEvent struct {
	EventType string `json:"eventType"`
	ChatId    string `json:"chatId"`
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file")
	}
	botToken := os.Getenv("TELEGRAM_BOT_KEY")
	if botToken == "" {
		log.Fatalln("No telegram bot token provided")
	}
	chatIdStr := os.Getenv("CHAT_ID")
	if chatIdStr == "" {
		log.Fatalln("No chat id was provided")
	}
	chatId, err := strconv.Atoi(chatIdStr)
	if err != nil {
		log.Fatalln("Invalid chat id provided, expected number got", chatIdStr)
	}
	// Kafka stuff
	kafkaMsgChan := make(chan []byte)
	startConsuming, stopConsuming := consuming.NewConsumer(kafkaMsgChan, os.Getenv("GROUP_ID"))
	go startConsuming()
	defer stopConsuming()

	// Telegram stuff
	bot, err := tgbotapi.NewBotAPI(botToken)

	if err != nil {
		log.Panic(err)
	}

	for kafkaMsg := range kafkaMsgChan {
		var event ChatStartedEvent
		if err := json.Unmarshal(kafkaMsg, &event); err != nil {
			log.Println("Unable to Unmarshal event because of error", err.Error())
			continue
		}
		if event.EventType == "ChatStartedEvent" {
			url := "https://max.netterberg.com/chat/" + event.ChatId
			text := fmt.Sprintf("Hey, someone wants to chat with you!\nVisit %s to chat with them!", url)

			msg := tgbotapi.NewMessage(int64(chatId), text)
			bot.Send(msg)
		}
	}

}
