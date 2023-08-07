#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <cstddef>

enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
};

enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};

class Statement {
public:
    StatementType type;
};

class InputBuffer {
public:
    char* buffer;
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
    void read_input(InputBuffer* input_buffer);
    MetaCommandResult do_meta_command(InputBuffer* input_buffer);
    PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement);
    void close_input_buffer(InputBuffer* input_buffer) {
        delete input_buffer;
    }
    void execute_statement(Statement* statement);
    void run();
};

void Database::read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        std::cout << "Error reading input\n";
        exit(EXIT_FAILURE);
    }
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

MetaCommandResult Database::do_meta_command(InputBuffer* input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        std::cout << "Bye!\n";
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult Database::prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    } else if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    } else {
        return PREPARE_UNRECOGNIZED_STATEMENT;
    }
}

void Database::execute_statement(Statement* statement) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            std::cout << "Executing insert statement.\n";
            break;
        case (STATEMENT_SELECT):
            std::cout << "Executing select statement.\n";
            break;
    }
}

void Database::run() {
    InputBuffer* input_buffer = new InputBuffer{};
    while (true) {
        print_prompt();
        read_input(input_buffer);
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    std::cout << "Unrecognized command " << input_buffer->buffer << std::endl;
                    continue;
            }
        }
        Statement statement{};
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                std::cout << "Unrecognized keyword at start of " << "'" << input_buffer->buffer << "'." << std::endl;
                continue;
        }
        execute_statement(&statement);
        std::cout << "Executed successfully.\n";
    }
}

int main() {
    Database db{};
    db.run();
    return 0;
}