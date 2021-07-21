package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
)

type Events struct {
	id        string
	eventType string
	actorName string
	repoName  string
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
	watchEventsSorted, pushEventsSorted, userPullRequestsSorted, commitsSorted := readRawEvents(eventsRaw, commitsRaw, actorMap, repoMap)

	// I know this is looking kinda ugly, but I wanted to keep it simple
	fmt.Println("Watch Events By Repositories: ")
	fmt.Println(watchEventsSorted)
	fmt.Println("------------------------------")
	fmt.Println("Push Events By Repositories: ")
	fmt.Println(pushEventsSorted)
	fmt.Println("------------------------------")
	fmt.Println("Push Events By Users: ")
	fmt.Println(userPullRequestsSorted)
	fmt.Println("------------------------------")
	fmt.Println("Commits By Users: ")
	fmt.Println(commitsSorted)
	fmt.Println("------------------------------")
}

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		panic("Unable to read file " + filePath + " " + err.Error())
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	result, err := csvReader.ReadAll()
	if err != nil {
		panic("Unable to parse CSV file : " + filePath + " " + err.Error())
	}

	return result
}

func checkEventFrequency(controllerValue string, events map[string]int) map[string]int {
	_, ok := events[controllerValue]
	if !ok {
		events[controllerValue] = 1
	} else {
		events[controllerValue]++
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

	if len(result) < 10 {
		return result
	}

	return result[0:10]
}

// This function should only be used with 2 column CSV files
func rawDataParser(rawData [][]string) map[string]string {
	result := make(map[string]string)
	for _, data := range rawData {
		result[data[0]] = data[1]
	}
	return result
}

func getCommitsByUser(rawCommitData [][]string, eventsWithUserID map[string]string) map[string]int {
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
	return userCommits
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
		event.actorName = actorMap[data[2]]
		event.repoName = repoMap[data[3]]
		if event.eventType == "WatchEvent" {
			watchEvents = checkEventFrequency(event.repoName, watchEvents)
		} else if event.eventType == "PushEvent" {
			pushEvents = checkEventFrequency(event.repoName, pushEvents)
		} else if event.eventType == "PullRequestEvent" {
			userPREvents = checkEventFrequency(event.actorName, userPREvents)
		}
		eventsWithUserID[event.id] = event.actorName
	}

	userCommits := getCommitsByUser(rawCommitData, eventsWithUserID)

	watchEventsSorted := mapSorter(watchEvents)
	pushEventsSorted := mapSorter(pushEvents)
	userPullRequestsSorted := mapSorter(userPREvents)
	commitsSorted := mapSorter(userCommits)

	return watchEventsSorted, pushEventsSorted, userPullRequestsSorted, commitsSorted
}
