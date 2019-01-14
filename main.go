package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os/exec"
	"strings"
)

const ChunkSize = 1000

type Sql struct {
	query string
	count string
}

type Dimension struct {
	team    bool
	project bool
	user    bool
}

func (c *Dimension) Suffix() string {
	sql := ""
	if c.team {
		sql += " AND team_id <> 0"
	} else {
		sql += " AND team_id = 0"
	}
	if c.project {
		sql += " AND project_id <> 0"
	} else {
		sql += " AND project_id = 0"
	}
	if c.user {
		sql += " AND user_id <> 0"
	} else {
		sql += " AND user_id = 0"
	}
	return sql
}

type Measurement struct {
	code      int
	value     string
	dimension []Dimension
	sql       []Sql
}

func (m *Measurement) Sql() []Sql {
	var ret []Sql
	sql := " SELECT team_id team,project_id project,user_id user,count value,UNIX_TIMESTAMP(date) * 1000000000 time FROM statistic_records WHERE type = ? AND count > 0 "
	count := " SELECT COUNT(1) FROM statistic_records WHERE type = ? AND count > 0 "
	for _, dim := range m.dimension {
		ret = append(ret, Sql{
			query: sql + dim.Suffix(),
			count: count + dim.Suffix(),
		})
	}
	return ret
}

type Importer struct {
	measurement []Measurement
	database    string
	influxURL   string
}

func newImport(database string, influxURL string) Importer {
	var mapping = []Measurement{
		{
			code:  28,
			value: "code_commit_inc",
			dimension: []Dimension{
				{team: true, project: false, user: false},
				{team: true, project: true, user: false},
				{team: true, project: true, user: true},
			},
		},
		{
			code:  30,
			value: "code_line_del_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
				{team: true, project: true, user: true},
			},
		},
		{
			code:  32,
			value: "code_line_modify_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
				{team: true, project: true, user: true},
			},
		},
		{
			code:  29,
			value: "code_line_new_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
				{team: true, project: true, user: true},
			},
		},
		{
			code:  64,
			value: "wiki_share_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
			},
		},
		{
			code:  83,
			value: "file_download_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
			},
		},
		{
			code:  82,
			value: "file_share_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
			},
		},
		{
			code:  93,
			value: "member_add_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
			},
		},
		{
			code:  94,
			value: "member_remove_inc",
			dimension: []Dimension{
				{team: true, project: true, user: false},
			},
		},
		{
			code:  31,
			value: "code_mr_review_inc",
			dimension: []Dimension{
				{team: true, project: true, user: true},
			},
		},
	}

	return Importer{
		measurement: mapping,
		database:    database,
		influxURL:   influxURL,
	}
}

func (c *Importer) run() {
	for _, m := range c.measurement {
		c.read(m)
	}
}

func (c *Importer) read(measurement Measurement) {
	db, err := sql.Open("mysql", c.database)
	defer db.Close()
	if err != nil {
		log.Panicln("Open database connect error", err)
	}
	for _, s := range measurement.Sql() {
		log.Println("Start read measurement " + measurement.value + " use query: " + s.query)
		c.readRow(db, measurement, s)
	}
}

func (c *Importer) readRow(db *sql.DB, measurement Measurement, sql Sql) {
	var (
		team    string
		project string
		user    string
		value   string
		time    string
		count   int32
	)

	rows, err := db.Query(sql.count, measurement.code)
	defer rows.Close()
	if err != nil {
		log.Panicln("Read measurement " + measurement.value + " count " + err.Error())
	}
	for rows.Next() {
		rows.Scan(&count)
		if err != nil {
			log.Panicln("Read count error " + err.Error())
		}
	}

	rows, err = db.Query(sql.query, measurement.code)
	defer rows.Close()
	if err != nil {
		log.Panicln("Read measurement " + measurement.value + " " + err.Error())
	}
	var binary []string
	i := 0
	for rows.Next() {
		err := rows.Scan(&team, &project, &user, &value, &time)
		if err != nil {
			log.Panicln("Read row error " + err.Error())
		}
		binary = append(binary, fmt.Sprintln(measurement.value+",team_id="+team+",project_id="+project+",user_id="+user+" value="+value+" "+time))
		if len(binary) == ChunkSize {
			i += ChunkSize
			fmt.Printf("\rWrite date to influxdb %d/%d", i, count)
			c.write(strings.Join(binary, ""))
			binary = make([]string, 0)
		}
	}
	i += len(binary)
	fmt.Printf("\rWrite date to influxdb %d/%d", i, count)
	c.write(strings.Join(binary, ""))
	binary = make([]string, 1)
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("")
}

func (c *Importer) write(binary string) {
	cmd := exec.Command("curl", "-i", "-XPOST", c.influxURL, "-d", binary)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Printf("err:\n%s\n", string(stderr.Bytes()))
	}
}

func main() {
	database := flag.String("database", "", "Mysql 数据库连接地址：root:123@tcp(127.0.0.1:3306)/coding_statistic")
	influxURL := flag.String("influx-url", "", "influxdb 数据库连接地址：http://127.0.0.1:8086/write?db=statistic&u=root&p=coding123")
	flag.Parse()

	if len(*database) <= 0 ||
		len(*influxURL) <= 0 {
		log.Fatalf("Usage : influx-import --host --port --databse --username --password")
	}

	importer := newImport(*database, *influxURL)
	importer.run()
}
