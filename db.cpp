#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <cstddef>
#include <cstdint>

enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
};

enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};

enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
};

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

class Row {
public:
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];

    Row() {
        id = 0;
        username[0] = '\0';
        email[0] = '\0';
    }
    Row(uint32_t id, const char *username, const char *email) {
        this->id = id;
        strncpy(this->username, username, COLUMN_USERNAME_SIZE + 1);
        strncpy(this->email, email, COLUMN_EMAIL_SIZE + 1);
    }
};

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serialize_row(Row* source, void *destination) {
    memcpy((char *)destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy((char *)destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy((char *)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

class Table {
public:
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
    Table() {
        num_rows = 0;
        for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
            pages[i] = nullptr;
        }
    }
    ~Table() {
        for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
            free(pages[i]);
        }
    }
};

void *row_slot(Table *table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if (page == nullptr) {
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return (char *)page + byte_offset;
}

class Statement {
public:
    StatementType type;
    Row row_to_insert;
};

class InputBuffer {
public:
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;

    InputBuffer(): buffer{nullptr}, buffer_length{0}, input_length{0} {}
    ~InputBuffer() {
        delete[] buffer;
    }
};

class Database {
public:
    void print_prompt() {
        std::cout << "db > ";
    }
    void read_input(InputBuffer *input_buffer);
    MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
    PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
    PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);
    void close_input_buffer(InputBuffer *input_buffer) {
        delete input_buffer;
    }
    void close_table(Table *table) {
        delete table;
    }
    ExecuteResult execute_insert(Statement *statement, Table *table);
    ExecuteResult execute_select(Statement *statement, Table *table);
    ExecuteResult execute_statement(Statement *statement, Table *table);
    void run();
};

void Database::read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        std::cout << "Error reading input\n";
        exit(EXIT_FAILURE);
    }
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

MetaCommandResult Database::do_meta_command(InputBuffer *input_buffer, Table *table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        std::cout << "Bye!\n";
        close_input_buffer(input_buffer);
        close_table(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult Database::prepare_insert(InputBuffer *input_buffer, Statement *statement) {
    statement->type = STATEMENT_INSERT;
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");
    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = strtol(id_string, (char **)NULL, 10);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert = Row(id, username, email);
    return PREPARE_SUCCESS;
}

PrepareResult Database::prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    } else if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    } else {
        return PREPARE_UNRECOGNIZED_STATEMENT;
    }
}

ExecuteResult Database::execute_insert(Statement *statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    serialize_row(&(statement->row_to_insert), row_slot(table, table->num_rows));
    table->num_rows++;
    return EXECUTE_SUCCESS;
}

ExecuteResult Database::execute_select(Statement *statement, Table* table) {
    Row *row = new Row{};
    for (uint32_t i = 0; i < table->num_rows; ++i) {
        deserialize_row(row_slot(table, i), row);
        std::cout << "(" << row->id << ", " << row->username << ", " << row->email << ")" << std::endl;
    }
    delete row;
    return EXECUTE_SUCCESS;
}

ExecuteResult Database::execute_statement(Statement *statement, Table *table) {
    if (statement->type == STATEMENT_INSERT) {
        return execute_insert(statement, table);
    } else {
        return execute_select(statement, table);
    }
}

void Database::run() {
    InputBuffer *input_buffer = new InputBuffer{};
    Table *table = new Table{};
    Statement *statement = new Statement{};

    while (true) {
        print_prompt();
        read_input(input_buffer);
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    std::cout << "Unrecognized command " << input_buffer->buffer << std::endl;
                    continue;
            }
        }
        switch (prepare_statement(input_buffer, statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                std::cout << "ID must be positive.\n";
                continue;
            case (PREPARE_STRING_TOO_LONG):
                std::cout << "String is too long.\n";
                continue;
            case (PREPARE_SYNTAX_ERROR):
                std::cout << "Syntax error. Could not parse statement.\n";
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                std::cout << "Unrecognized keyword at start of " << "'" << input_buffer->buffer << "'." << std::endl;
                continue;
        }
        switch (execute_statement(statement, table)) {
            case (EXECUTE_SUCCESS):
                std::cout << "Executed.\n";
                break;
            case (EXECUTE_TABLE_FULL):
                std::cout << "Error: Table full.\n";
                break;
        }
    }
}

int main() {
    Database db{};
    db.run();
    return 0;
}