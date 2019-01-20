package main

import (
	"encoding/json"
	"log"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/influxdata/influxdb1-client/models"
	"github.com/influxdata/influxdb1-client/v2"
)

var (
	batchSize      = kingpin.Flag("batch-size", "Number of metrics inserted at a time").Default("10000").Int()
	sourceUsername = kingpin.Flag("source-username", "Username for the source InfluxDB.").Required().String()
	sourcePassword = kingpin.Flag("source-password", "Password for the source InfluxDB.").Required().String()
	targetUsername = kingpin.Flag("target-username", "Username for the target InfluxDB. If missing, source-username is used.").Default(*sourceUsername).String()
	targetPassword = kingpin.Flag("target-password", "Password for the target InfluxDB. If missing, source-password is used.").Default(*sourcePassword).String()
	sourceURL      = kingpin.Flag("source-url", "URL of the source InfluxDB.").Default("https://influxdb.hq.grdl.pl").String()
	targetURL      = kingpin.Flag("target-url", "URL of the target InfluxDB. If missing, source-url is used.").Default(*sourceURL).String()
	sourceDB       = kingpin.Flag("source-db", "Name of the source database.").Default("nestats").String()
	targetDB       = kingpin.Flag("target-db", "Name of the target database.").Default("prometheus").String()
)

var (
	defaultTags = map[string]string{
		"job":      "pronestheus",
		"instance": "pronestheus:2112",
		"name":     "Living-Room",
		"id":       "JyHyG8n7kBXBV0_KHqQhNsmUnpmzy3o_",
	}

	//Columns to convert:

	// inside:
	// has_leaf -> nest_leaf
	// humidity ->  nest_humidity
	// is_heating -> nest_heating
	// target -> nest_target_temp
	// temperature -> nest_current_temp

	insideQuery      = "select has_leaf as nest_leaf, humidity as nest_humidity, is_heating as nest_heating, target as nest_target_temp, temperature as nest_current_temp from inside where time > now() -10h"
	insideCountQuery = "select count(*) from inside where time > now() -10h"

	// outside:
	// humidity -> nest_weather_humidity
	// pressure -> nest_weather_pressure
	// temperature -> nest_weather_temp

	outsideQuery      = "select humidity as nest_weather_humidity, pressure as nest_weather_pressure, temperature as nest_weather_temp from outside where time > now() -10h"
	outsideCountQuery = "select count(*) from outside where time > now() -10h"
)

type Converter struct {
	SourceClient client.Client
	TargetClient client.Client
}

func main() {
	kingpin.Parse()

	c, err := NewConverter()
	if err != nil {
		log.Fatal(err)
	}

	c.RunOnTable(insideQuery, insideCountQuery)
	c.RunOnTable(outsideQuery, outsideCountQuery)
}

func NewConverter() (*Converter, error) {
	sourceConf := client.HTTPConfig{
		Addr:     *sourceURL,
		Username: *sourceUsername,
		Password: *sourcePassword,
	}

	targetConf := client.HTTPConfig{
		Addr:     *targetURL,
		Username: *targetUsername,
		Password: *targetPassword,
	}

	sourceClient, err := client.NewHTTPClient(sourceConf)
	if err != nil {
		return nil, err
	}

	targetClient, err := client.NewHTTPClient(targetConf)
	if err != nil {
		return nil, err
	}

	return &Converter{
		SourceClient: sourceClient,
		TargetClient: targetClient,
	}, nil
}

func (c *Converter) RunOnTable(query string, countQuery string) {
	log.Println("-------------------------------")
	log.Printf("Running with query: %s\n", query)

	result, err := c.Query(*sourceDB, countQuery)
	if err != nil {
		log.Fatal(err)
	}

	countedQuery := result.Values[0][1]

	result, err = c.Query(*sourceDB, query)
	if err != nil {
		log.Fatal(err)
	}

	countedResult := len(result.Values)

	log.Printf("Query counted %s metrics and received %d metrics\n", countedQuery, countedResult)

	var batches [][][]interface{}
	for *batchSize < len(result.Values) {
		result.Values, batches = result.Values[*batchSize:], append(batches, result.Values[0:*batchSize:*batchSize])
	}
	batches = append(batches, result.Values)

	for i, batch := range batches {
		log.Printf("Converting batch %d / %d\n", i+1, len(batches))

		points, err := c.Convert(result.Columns, batch)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Writing batch %d / %d\n", i+1, len(batches))

		err = c.WritePoints(points)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (c *Converter) Query(database string, query string) (rows models.Row, err error) {
	result := models.Row{}

	q := client.Query{
		Command:   query,
		Database:  database,
		Chunked:   true,
		ChunkSize: 10000,
		Precision: "s",
	}

	response, err := c.SourceClient.Query(q)

	if err != nil {
		return result, err
	}

	if response.Error() != nil {
		return result, response.Error()
	}

	if len(response.Results) != 1 && len(response.Results[0].Series) != 1 {
		log.Fatalf("Something went wrong: received %d results and %d series in the first result from the query %s\n",
			len(response.Results),
			len(response.Results[0].Series),
			query)
	}

	result = response.Results[0].Series[0]

	return result, nil
}

func (c *Converter) Convert(columns []string, batch [][]interface{}) (points []*client.Point, err error) {
	// Go over each column and convert it into a separate measurement
	for _, values := range batch {
		timeValue, err := values[0].(json.Number).Int64()
		if err != nil {
			return nil, err
		}

		timestamp := time.Unix(timeValue, 0)

		// skip the columns[0], that's the timestamp
		for i := 1; i < len(columns); i++ {
			value, err := values[i].(json.Number).Float64()
			if err != nil {
				return nil, err
			}

			point, err := c.newPoint(columns[i], timestamp, value)
			if err != nil {
				return nil, err
			}
			points = append(points, point)
		}
	}

	return points, nil
}

func (c *Converter) newPoint(name string, timestamp time.Time, value float64) (point *client.Point, err error) {
	tags := map[string]string{
		"__name__": name,
	}

	for k, v := range defaultTags {
		tags[k] = v
	}

	fields := map[string]interface{}{
		"value": value,
	}

	point, err = client.NewPoint(name, tags, fields, timestamp)

	return point, nil
}

func (c *Converter) WritePoints(points []*client.Point) (err error) {
	batchPoints, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  *targetDB,
		Precision: "s",
	})

	batchPoints.AddPoints(points)

	err = c.TargetClient.Write(batchPoints)
	return err
}
