package main

import "testing"

func TestExtractKeyword(t *testing.T) {
	out := extractKeyword("hello world KEYWORD[hello,_world]")
	if out != "hello,_world" {
		t.Fatal("failed 1")
	}
	out = extractKeyword("hello world KEYWORD[hello,_world] dust hello KEYWORD[dust,hello]")
	if out != "hello,_world,dust,hello" {
		t.Fatal("failed 2")
	}
	out = extractKeyword("KEYWORD[dust,world] hello world KEYWORD[hello,_world] dust hello KEYWORD[dust,hello]")
	if out != "dust,world,hello,_world,dust,hello" {
		t.Fatal("failed 2")
	}
}
