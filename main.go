package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
)

type Events struct {
	id        string
	eventType string
	actorID   string
	repoID    string
}

type EventFrequency struct {
	id             string
	eventFrequency int
}

func main() {

	actorsRaw := readCsvFile("data/actors.csv")
	reposRaw := readCsvFile("data/repos.csv")
	eventsRaw := readCsvFile("data/events.csv")
	commitsRaw := readCsvFile("data/commits.csv")

	actorMap := rawDataParser(actorsRaw)
	repoMap := rawDataParser(reposRaw)
	watchEventsSorted, pushEventsSorted, userPushEventsSorted, commitsSorted := readRawEvents(eventsRaw, commitsRaw, actorMap, repoMap)

	fmt.Println("Watch Events By Repositories: ")
	fmt.Println(watchEventsSorted)
	fmt.Println("------------------------------")
	fmt.Println("Push Events By Repositories: ")
	fmt.Println(pushEventsSorted)
	fmt.Println("------------------------------")
	fmt.Println("Push Events By Users: ")
	fmt.Println(userPushEventsSorted)
	fmt.Println("------------------------------")
	fmt.Println("Commits By Users: ")
	fmt.Println(commitsSorted)
	fmt.Println("------------------------------")
}

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	result, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse CSV file : "+filePath, err)
	}

	return result
}

func checkEventFrequencyByRepoID(event Events, events map[string]int) map[string]int {
	_, ok := events[event.repoID]
	if !ok {
		events[event.repoID] = 1
	} else {
		events[event.repoID]++
	}
	return events
}

func checkEventFrequencyByActorID(event Events, events map[string]int) map[string]int {
	_, ok := events[event.actorID]
	if !ok {
		events[event.actorID] = 1
	} else {
		events[event.actorID]++
	}
	return events
}

func mapSorter(events map[string]int) []EventFrequency {
	result := make([]EventFrequency, len(events))
	i := 0
	for key, value := range events {
		result[i] = EventFrequency{
			id:             key,
			eventFrequency: value,
		}
		i++
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].eventFrequency > result[j].eventFrequency
	})

	return result[0:10]
}

func rawDataParser(rawData [][]string) map[string]string {
	result := make(map[string]string)
	for _, data := range rawData {
		result[data[0]] = data[1]
	}
	return result
}

func readRawEvents(rawData, rawCommitData [][]string, actorMap, repoMap map[string]string) ([]EventFrequency, []EventFrequency, []EventFrequency, []EventFrequency) {
	var event Events
	watchEvents := make(map[string]int)
	pushEvents := make(map[string]int)
	userPREvents := make(map[string]int)
	eventsWithUserID := make(map[string]string)
	for _, data := range rawData {
		event.id = data[0]
		event.eventType = data[1]
		event.actorID = actorMap[data[2]]
		event.repoID = repoMap[data[3]]
		if event.eventType == "WatchEvent" {
			watchEvents = checkEventFrequencyByRepoID(event, watchEvents)
		} else if event.eventType == "PushEvent" {
			pushEvents = checkEventFrequencyByRepoID(event, pushEvents)
		} else if event.eventType == "PullRequestEvent" {
			userPREvents = checkEventFrequencyByActorID(event, userPREvents)
		}
		eventsWithUserID[event.id] = event.actorID
	}

	userCommits := make(map[string]int)
	for _, commit := range rawCommitData {
		eventID := commit[2]
		_, ok := userCommits[eventsWithUserID[eventID]]
		if !ok {
			userCommits[eventsWithUserID[eventID]] = 1
		} else {
			userCommits[eventsWithUserID[eventID]]++
		}
	}

	watchEventsSorted := mapSorter(watchEvents)
	pushEventsSorted := mapSorter(pushEvents)
	userPushEventsSorted := mapSorter(userPREvents)
	commitsSorted := mapSorter(userCommits)

	return watchEventsSorted, pushEventsSorted, userPushEventsSorted, commitsSorted
}
