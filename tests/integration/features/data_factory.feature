Feature: Data Factory

    Background:
        Given there are airports:
            | 1 | Heathrow | London | UK | LHR | LHR | 1.0 | 1.0 | 0.0 | | | | airport | manual |

    Scenario: Transform
        Given a source csv file
        When it is transformed
        Then a table is created