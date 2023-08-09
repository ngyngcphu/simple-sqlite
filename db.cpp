#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
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
    strncpy((char *)destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy((char *)destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
    memcpy(&(destination->id), (char *)source + ID_OFFSET, ID_SIZE);
    strncpy(destination->username, (char *)source + USERNAME_OFFSET, USERNAME_SIZE);
    strncpy(destination->email, (char *)source + EMAIL_OFFSET, EMAIL_SIZE);
}

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

class Pager {
public:
    int file_description;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];

    Pager(const char *filename);
    void *get_page(uint32_t page_num);
    void pager_flush(uint32_t page_num, uint32_t size);
};

Pager::Pager(const char *filename) {
    file_description = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (file_description < 0) {
        std::cerr << "Error: cannot open file " << filename << std::endl;
        exit(EXIT_FAILURE);
    }
    file_length = lseek(file_description, 0, SEEK_END);
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pages[i] = nullptr;
    }
}

void *Pager::get_page(uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        std::cout << "Tried to fetch page number out of bounds. " << page_num << " > "
                    << TABLE_MAX_PAGES << std::endl;
        exit(EXIT_FAILURE);
    }
    if (pages[page_num] == nullptr) {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = file_length / PAGE_SIZE;
        if (file_length % PAGE_SIZE != 0) {
            num_pages++;
        }
        if (page_num <= num_pages) {
            lseek(file_description, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(file_description, page, PAGE_SIZE);
            if (bytes_read == -1) {
                std::cout << "Error reading file: " << errno << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        pages[page_num] = page;
    }
    return pages[page_num];
}

void Pager::pager_flush(uint32_t page_num, uint32_t size) {
    if (pages[page_num] == nullptr) {
        std::cout << "Tried to flush null page\n";
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(file_description, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        std::cout << "Error seeking: " << errno << std::endl;
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(file_description, pages[page_num], size);
    if (bytes_written == -1) {
        std::cout << "Error writting: " << errno << std::endl;
        exit(EXIT_FAILURE);
    }
}

class Table {
public:
    uint32_t num_rows;
    Pager *pager;
    Table(const char *filename) {
        pager = new Pager{ filename };
        num_rows = pager->file_length / ROW_SIZE;
    }
    ~Table();
};

Table::~Table() {
    uint32_t num_full_pages = num_rows / ROWS_PER_PAGE;
    for (uint32_t i = 0; i < num_full_pages; ++i) {
        if (pager->pages[i] == nullptr) {
            continue;
        }
        pager->pager_flush(i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = nullptr;
    }
    uint32_t num_addtional_rows = num_rows % ROWS_PER_PAGE;
    if (num_addtional_rows > 0) {
        if (pager->pages[num_full_pages] != nullptr) {
            pager->pager_flush(num_full_pages, num_addtional_rows * ROW_SIZE);
            free(pager->pages[num_full_pages]);
            pager->pages[num_full_pages] = nullptr;
        }
    }
    int result = close(pager->file_description);
    if (result == -1) {
        std::cout << "Error closing db file.\n";
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        void *page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = nullptr;
        }
    }
    delete pager;
}

class Cursor {
public:
    Table *table;
    uint32_t row_num;
    bool end_of_table;

    Cursor(Table *table, bool option);
    void *cursor_value();
    void cursor_advance();
};

Cursor::Cursor(Table *table, bool option) {
    this->table = table;
    if (option) {
        row_num = 0;
        end_of_table = (table->num_rows == 0);
    } else {
        row_num = table->num_rows;
        end_of_table = true;
    }
}

void *Cursor::cursor_value() {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pager->get_page(page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return (char *)page + byte_offset;
}

void Cursor::cursor_advance() {
    ++row_num;
    if (row_num >= table->num_rows) {
        end_of_table = true;
    }
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
        buffer = nullptr;
    }
};

class Database {
private:
    Table *table;

public:
    Database(const char *filename) {
        table = new Table{ filename };
    }
    void print_prompt() {
        std::cout << "db > ";
    }
    void read_input(InputBuffer *input_buffer);
    MetaCommandResult do_meta_command(InputBuffer *input_buffer);
    PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
    PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);
    ExecuteResult execute_insert(Statement *statement);
    ExecuteResult execute_select(Statement *statement);
    ExecuteResult execute_statement(Statement *statement);
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

MetaCommandResult Database::do_meta_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        delete input_buffer;
        delete table;
        input_buffer = nullptr;
        table = nullptr;
        std::cout << "Bye!\n";
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

ExecuteResult Database::execute_insert(Statement *statement) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Cursor *cursor = new Cursor{table, false};
    serialize_row(&(statement->row_to_insert), cursor->cursor_value());
    table->num_rows++;
    delete cursor;
    return EXECUTE_SUCCESS;
}

ExecuteResult Database::execute_select(Statement *statement) {
    Row *row = new Row{};
    Cursor *cursor = new Cursor{ table, true };
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor->cursor_value(), row);
        std::cout << "(" << row->id << ", " << row->username << ", " << row->email << ")" << std::endl;
        cursor->cursor_advance();
    }
    delete row;
    delete cursor;
    row = nullptr;
    cursor = nullptr;
    return EXECUTE_SUCCESS;
}

ExecuteResult Database::execute_statement(Statement *statement) {
    if (statement->type == STATEMENT_INSERT) {
        return execute_insert(statement);
    } else {
        return execute_select(statement);
    }
}

void Database::run() {
    InputBuffer *input_buffer = new InputBuffer{};
    Statement *statement = new Statement{};

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
        switch (execute_statement(statement)) {
            case (EXECUTE_SUCCESS):
                std::cout << "Executed.\n";
                break;
            case (EXECUTE_TABLE_FULL):
                std::cout << "Error: Table full.\n";
                break;
        }
    }
}

int main(int argc, const char *argv[]) {
    if (argc < 2) {
        std::cout << "Must supply a database filename." << std::endl;
        exit(EXIT_FAILURE);
    }
    Database db{ argv[1] };
    db.run();
    return 0;
}