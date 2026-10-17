package yandex.praktikum;

public class Message {

    public String recipientId;
    public String text;

    public Message() {
    }

    public Message(String recipientId, String text) {
        this.recipientId = recipientId;
        this.text = text;
    }
}