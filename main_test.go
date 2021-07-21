package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ReadCsvFile_Success(t *testing.T) {
	actorsRaw := readCsvFile("data/actors.csv")

	require.NotPanics(t, func() {
		readCsvFile("data/actors.csv")
	})
	require.NotNil(t, actorsRaw)
}

func Test_ReadCsvFile_NoSuchFile(t *testing.T) {
	require.PanicsWithValuef(t, "Unable to read file data/actorssss.csv open data/actorssss.csv: no such file or directory", func() {
		readCsvFile("data/actorssss.csv")
	}, "")
}

func Test_ReadCsvFile_NonCSVFile(t *testing.T) {
	require.Panics(t, func() {
		readCsvFile("data/helloThere.jpg")
	}, "")
}

func Test_CheckEventFrequency_Success(t *testing.T) {
	events := make(map[string]int)
	dummyEventOne := Events{
		id:        "1",
		eventType: "PushEvent",
		actorName: "1234",
		repoName:  "4321",
	}
	dummyEventTwo := Events{
		id:        "2",
		eventType: "PushEvent",
		actorName: "1234",
		repoName:  "4321",
	}
	dummyEventThree := Events{
		id:        "3",
		eventType: "PushEvent",
		actorName: "1234",
		repoName:  "00000",
	}
	events = checkEventFrequency(dummyEventOne.repoName, events)
	events = checkEventFrequency(dummyEventTwo.repoName, events)
	events = checkEventFrequency(dummyEventThree.repoName, events)

	require.NotNil(t, events)
	require.Equal(t, events["4321"], 2)
}

func Test_MapSorter_Success(t *testing.T) {
	events := make(map[string]int)
	dummyEventOne := Events{
		id:        "1",
		eventType: "PushEvent",
		actorName: "1234",
		repoName:  "4321",
	}
	dummyEventTwo := Events{
		id:        "2",
		eventType: "PushEvent",
		actorName: "1234",
		repoName:  "4321",
	}
	dummyEventThree := Events{
		id:        "3",
		eventType: "PushEvent",
		actorName: "1",
		repoName:  "4321",
	}

	events = checkEventFrequency(dummyEventOne.actorName, events)
	events = checkEventFrequency(dummyEventTwo.actorName, events)
	events = checkEventFrequency(dummyEventThree.actorName, events)

	result := mapSorter(events)
	expectedResult := map[string]int{"1": 1, "1234": 2}

	require.NotNil(t, result)
	require.Equal(t, expectedResult, events)
}

func Test_RawDataParser_Success(t *testing.T) {
	rawData := [][]string{
		{"1", "dummyOne"},
		{"2", "dummyTwo"},
		{"3", "dummyThree"},
	}

	result := rawDataParser(rawData)
	expectedResult := map[string]string{"1": "dummyOne", "2": "dummyTwo", "3": "dummyThree"}

	require.NotNil(t, result)
	require.Equal(t, expectedResult, result)
}

func Test_GetCommitsByUser_Success(t *testing.T) {
	eventsWithUserID := make(map[string]string)
	eventsWithUserID["1001"] = "UserOne"
	eventsWithUserID["1002"] = "UserTwo"

	rawCommitData := [][]string{
		{"1", "commitOne", "1001"},
		{"2", "commitTwo", "1002"},
		{"3", "commitThree", "1001"},
	}

	result := getCommitsByUser(rawCommitData, eventsWithUserID)
	expectedResult := map[string]int{"UserOne": 2, "UserTwo": 1}

	require.NotNil(t, result)
	require.Equal(t, expectedResult, result)
}

func Test_ReadRawEvents_Success(t *testing.T) {
	actorMap := make(map[string]string)
	actorMap["1001"] = "UserOne"
	actorMap["1002"] = "UserTwo"

	repoMap := make(map[string]string)
	repoMap["1"] = "atilpasabeyoglu/StarWars"
	repoMap["2"] = "atilpasabeyoglu/LordOfTheRings"

	rawData := [][]string{
		{"1", "PushEvent", "1001", "1"},
		{"2", "PushEvent", "1002", "2"},
		{"3", "WatchEvent", "1001", "1"},
		{"4", "PushEvent", "1001", "1"},
		{"5", "PullRequestEvent", "1001", "1"},
		{"6", "PullRequestEvent", "1002", "2"},
	}

	rawCommitData := [][]string{
		{"1", "commitOne", "1"},
		{"2", "commitTwo", "1"},
		{"3", "commitThree", "2"},
	}

	watchEventsSorted, pushEventsSorted, userPullRequestsSorted, commitsSorted := readRawEvents(rawData, rawCommitData, actorMap, repoMap)
	expectedResultWatchEvents := []EventFrequency{{id: "atilpasabeyoglu/StarWars", eventFrequency: 1}}
	expectedPushEventsSorted := []EventFrequency{{id: "atilpasabeyoglu/StarWars", eventFrequency: 2}, {id: "atilpasabeyoglu/LordOfTheRings", eventFrequency: 1}}
	expectedUserPushEventsSorted := []EventFrequency{{id: "UserOne", eventFrequency: 1}, {id: "UserTwo", eventFrequency: 1}}
	expectedCommitsSorted := []EventFrequency{{id: "UserOne", eventFrequency: 2}, {id: "UserTwo", eventFrequency: 1}}

	require.NotNil(t, watchEventsSorted)
	require.NotNil(t, pushEventsSorted)
	require.NotNil(t, userPullRequestsSorted)
	require.NotNil(t, commitsSorted)

	require.Equal(t, expectedResultWatchEvents, watchEventsSorted)
	require.Equal(t, expectedPushEventsSorted, pushEventsSorted)
	require.Equal(t, expectedUserPushEventsSorted, userPullRequestsSorted)
	require.Equal(t, expectedCommitsSorted, commitsSorted)

}
