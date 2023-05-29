package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"unicode"
)

type WordFrequency struct {
	Word      []byte
	Frequency int
}

type SafeWordFrequencies struct {
	frequencies []WordFrequency
	mu          sync.Mutex
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please provide the path to the input file as an argument.")
		return
	}

	filePath := os.Args[1]
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Step 2: Count word frequencies
	frequencies := countWordFrequenciesParallel(scanner)

	// Step 4: Print top 20 words
	printTopWords(frequencies)

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
}

func countWordFrequenciesParallel(scanner *bufio.Scanner) []WordFrequency {
	// Determine the number of CPU cores
	numCPU := runtime.NumCPU()

	// Create a channel to receive word frequency results
	resultCh := make(chan []WordFrequency)

	// Read the scanner content into a buffer
	buffer := bytes.Buffer{}
	for scanner.Scan() {
		buffer.Write(scanner.Bytes())
	}

	// Split the buffer into chunks
	chunks := splitBufferIntoChunks(buffer, numCPU)

	// Create a mutex to synchronize access to the frequencies slice
	var mutex sync.Mutex
	safeFrequencies := SafeWordFrequencies{}

	// Start goroutines to process each chunk concurrently
	for _, chunk := range chunks {
		go func(chunkBuffer bytes.Buffer) {
			chunkScanner := bufio.NewScanner(&chunkBuffer)
			frequencies := countWordFrequencies(chunkScanner)

			// Safely merge the frequencies into the shared slice
			mutex.Lock()
			safeFrequencies.mergeFrequencies(frequencies)
			mutex.Unlock()

			resultCh <- frequencies
		}(chunk)
	}

	// Collect the results from goroutines
	for i := 0; i < numCPU; i++ {
		<-resultCh
	}

	// Return the final merged frequencies
	return safeFrequencies.getFrequencies()
}

func (sf *SafeWordFrequencies) mergeFrequencies(newFrequencies []WordFrequency) {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Merge the new frequencies into the existing slice
	for _, newFreq := range newFrequencies {
		existingIndex := -1
		for i, freq := range sf.frequencies {
			if equalWords(freq.Word, newFreq.Word) {
				existingIndex = i
				break
			}
		}

		if existingIndex != -1 {
			sf.frequencies[existingIndex].Frequency += newFreq.Frequency
		} else {
			sf.frequencies = append(sf.frequencies, newFreq)
		}
	}
}

func (sf *SafeWordFrequencies) getFrequencies() []WordFrequency {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Sort and trim the frequencies slice
	sort.Slice(sf.frequencies, func(i, j int) bool {
		return sf.frequencies[i].Frequency > sf.frequencies[j].Frequency
	})

	if len(sf.frequencies) > 20 {
		sf.frequencies = sf.frequencies[:20]
	}

	return sf.frequencies
}

func splitBufferIntoChunks(buffer bytes.Buffer, numChunks int) []bytes.Buffer {
	var chunks []bytes.Buffer

	// Get the total length of the buffer
	totalLength := buffer.Len()

	// Calculate the chunk size
	chunkSize := totalLength / numChunks

	// Split the buffer into chunks
	for i := 0; i < numChunks; i++ {
		chunkStart := i * chunkSize
		chunkEnd := chunkStart + chunkSize

		// Adjust the last chunk to include the remaining bytes
		if i == numChunks-1 {
			chunkEnd = totalLength
		}

		chunkData := buffer.Bytes()[chunkStart:chunkEnd]
		chunkBuffer := bytes.Buffer{}
		chunkBuffer.Write(chunkData)
		chunks = append(chunks, chunkBuffer)
	}

	return chunks
}

func countWordFrequencies(scanner *bufio.Scanner) []WordFrequency {
	var frequencies []WordFrequency

	for scanner.Scan() {
		line := scanner.Bytes()
		words := extractWords(line)
		for _, word := range words {
			incrementFrequency(&frequencies, word)
		}
	}

	return frequencies
}

func extractWords(line []byte) [][]byte {
	var words [][]byte
	var currentWord []byte

	for _, char := range line {
		if unicode.IsLetter(rune(char)) {
			currentWord = append(currentWord, byte(unicode.ToLower(rune(char))))
		} else if len(currentWord) > 0 {
			words = append(words, currentWord)
			currentWord = nil
		}
	}

	if len(currentWord) > 0 {
		words = append(words, currentWord)
	}

	return words
}

func incrementFrequency(frequencies *[]WordFrequency, word []byte) {
	for i := range *frequencies {
		if equalWords((*frequencies)[i].Word, word) {
			(*frequencies)[i].Frequency++
			return
		}
	}

	*frequencies = append(*frequencies, WordFrequency{Word: word, Frequency: 1})
}

func equalWords(word1, word2 []byte) bool {
	if len(word1) != len(word2) {
		return false
	}

	for i := range word1 {
		if word1[i] != word2[i] {
			return false
		}
	}

	return true
}

func printTopWords(frequencies []WordFrequency) {
	sort.Slice(frequencies, func(i, j int) bool {
		return frequencies[i].Frequency > frequencies[j].Frequency
	})

	for i := 0; i < 20 && i < len(frequencies); i++ {
		frequency := frequencies[i].Frequency

		fmt.Printf("%d\t", frequency)

		for _, v := range frequencies[i].Word {
			fmt.Printf("%c", v)
		}

		fmt.Print("\n")
	}
}
