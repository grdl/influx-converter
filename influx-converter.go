package main

import (
	"encoding/json"
	"net/url"
	"time"

	"github.com/influxdata/influxdb1-client/models"

	"log"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/influxdata/influxdb1-client"
)

var (
	username  = kingpin.Flag("username", "InfluxDB username").Required().String()
	password  = kingpin.Flag("password", "InfluxDB password").Required().String()
	influxURL = kingpin.Flag("url", "InfluxDB URL").Default("https://influxdb.hq.grdl.pl").String()
	batchSize = kingpin.Flag("batch-size", "Number of metrics inserted at a time").Default("100").Int()
	sourceDB  = kingpin.Flag("source-db", "Name of the source database").Default("nestats").String()
	targetDB  = kingpin.Flag("target-db", "Name of the target database").Default("prometheus").String()
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
	Client *client.Client
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
	host, err := url.Parse(*influxURL)
	if err != nil {
		return nil, err
	}

	conf := client.Config{
		URL:       *host,
		Username:  *username,
		Password:  *password,
		Precision: "s", // second precision is enough
	}

	con, err := client.NewClient(conf)
	if err != nil {
		return nil, err
	}

	return &Converter{
		Client: con,
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
		Command:  query,
		Database: database,
	}

	response, err := c.Client.Query(q)

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

func (c *Converter) Convert(columns []string, batch [][]interface{}) (points []client.Point, err error) {
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

			point := c.newPoint(columns[i], timestamp, value)
			points = append(points, point)
		}
	}

	return points, nil
}

func (c *Converter) newPoint(name string, timestamp time.Time, value float64) (point client.Point) {
	tags := map[string]string{
		"__name__": name,
	}

	for k, v := range defaultTags {
		tags[k] = v
	}

	point = client.Point{
		Measurement: name,
		Tags:        tags,
		Fields: map[string]interface{}{
			"value": value,
		},
		Time:      timestamp,
		Precision: "s",
	}

	return point
}

func (c *Converter) WritePoints(points []client.Point) error {
	batchPoints := client.BatchPoints{
		Points:   points,
		Database: *targetDB,
	}

	_, err := c.Client.Write(batchPoints)
	return err
}
