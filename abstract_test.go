package neogo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/rlch/neogo"
	"github.com/rlch/neogo/db"
	"github.com/rlch/neogo/internal"
)

func startNeo4J(ctx context.Context) (neo4j.DriverWithContext, func(context.Context) error) {
	request := testcontainers.ContainerRequest{
		Name:         "neo4j",
		Image:        "neo4j:5.7-enterprise",
		ExposedPorts: []string{"7687/tcp"},
		WaitingFor:   wait.ForLog("Bolt enabled").WithStartupTimeout(time.Minute * 2),
		Env: map[string]string{
			"NEO4J_AUTH":                     fmt.Sprintf("%s/%s", "neo4j", "password"),
			"NEO4J_PLUGINS":                  `["apoc"]`,
			"NEO4J_ACCEPT_LICENSE_AGREEMENT": "yes",
		},
	}
	container, err := testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: request,
			Started:          true,
			Reuse:            true,
		})
	if err != nil {
		panic(fmt.Errorf("container should start: %w", err))
	}

	port, err := container.MappedPort(ctx, "7687")
	if err != nil {
		panic(err)
	}
	uri := fmt.Sprintf("bolt://localhost:%d", port.Int())
	driver, err := neo4j.NewDriverWithContext(
		uri,
		neo4j.BasicAuth("neo4j", "password", ""),
	)
	if err != nil {
		panic(err)
	}
	return driver, container.Terminate
}

type Base struct {
	neogo.Abstract `neo4j:"Human"`
	neogo.Node
	IsAlive bool
}

func (Base) Implementers() []neogo.IAbstract {
	return []neogo.IAbstract{
		&Person{},
	}
}

type Person struct {
	Base `neo4j:"Person"`

	Name    string `json:"name"`
	Surname string `json:"surname"`
	Age     int    `json:"age"`
}

type Human interface {
	neogo.IAbstract
}

var _ Human = &Person{}

func TestArrayOfArray(t *testing.T) {
	ctx := context.Background()
	persons := []Person{
		{
			Base:    Base{Node: internal.Node{ID: "person1"}},
			Name:    "Spongebob",
			Surname: "Squarepants",
			Age:     20,
		},
		{
			Base:    Base{Node: internal.Node{ID: "person2"}},
			Name:    "Patrick",
			Surname: "Star",
			Age:     20,
		},
		{
			Base:    Base{Node: internal.Node{ID: "person3"}},
			Name:    "Spongebob",
			Surname: "Star",
			Age:     20,
		},
		{
			Base:    Base{Node: internal.Node{ID: "person4"}},
			Name:    "Patrick",
			Surname: "Squarepants",
			Age:     20,
		},
	}
	driver, terminate := startNeo4J(ctx)
	ng := neogo.New(driver, neogo.WithTypes(&Base{}))

	if err := ng.Exec().
		Cypher(`
UNWIND $persons AS person
CREATE (p:Person:Human {id: person.id, name: person.name, surname: person.surname, age: person.age})
RETURN p
		`).
		RunWithParams(ctx, map[string]any{
			"persons": persons,
		}); err != nil {
		panic(err)
	}

	surnames := []string{"Squarepants", "Star"}
	// Works if p2 is [][]any
	var p2 [][]any
	if err := ng.Exec().
		Cypher(`
UNWIND $surnames AS s
CALL {
	WITH s
	MATCH (p:Person {surname: s})
	RETURN coalesce(collect(properties(p)), []) AS p
}
		`).
		Return(db.Qual(&p2, "p")).
		Print().
		RunWithParams(ctx, map[string]any{
			"surnames": surnames,
		}); err != nil {
		panic(err)
	}
	for i := range p2 {
		if p2[i] != nil {
			fmt.Println(p2[i])
		}
	}

	// Single level works
	var onelevel []Human
	if err := ng.Exec().
		Cypher(`
MATCH (p:Human {surname: "Star"})
`).
		Return(db.Qual(&onelevel, "p")).
		Print().
		Run(ctx); err != nil {
		panic(err)
	}
	for i := range onelevel {
		fmt.Println(onelevel[i])
	}

	// Unmarshal error for [][]Human (interface implemented by Person)
	var p [][]Human
	if err := ng.Exec().
		Cypher(`
UNWIND $surnames AS s
CALL {
	WITH s
	MATCH (p:Human {surname: s})
	RETURN coalesce(collect(properties(p)), []) AS p
}
			`).
		Return(db.Qual(&p, "p")).
		Print().
		RunWithParams(ctx, map[string]any{
			"surnames": surnames,
		}); err != nil {
		panic(err)
	}
	fmt.Println(len(p))
	for i := range p {
		fmt.Println(p[i])
	}

	terminate(ctx)
}
