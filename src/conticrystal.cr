require "json"
require "sqlite3"

module Conticrystal
  class Message
    class TextEntity
      include JSON::Serializable

      getter type : String
      getter text : String
    end

    include JSON::Serializable

    getter id : Int32 | Int64
    getter type : String
    getter from_id : String?
    getter forwarded_from : String?
    getter text_entities : Array(TextEntity)

    @[JSON::Field(ignore: true)]
    property chat_id : Int64 = 0

    def words(&)
      last_is_punctuation = false
      @text_entities.each do |te|
        te.text.gsub(/\n+/, '.').scan(/[A-Za-zА-Яа-яЁ-ё]+|\.|!|\?|;|,|-|—|:/) do |md|
          yield md.to_s
          last_is_punctuation = [".", "!", "?"].includes? md.to_s
        end
      end
      yield "." if !last_is_punctuation
    end
  end

  class Dump
    getter chat_id : Int64

    def initialize(path : Path)
      @parser = JSON::PullParser.new File.new path
      @parser.read_begin_object
      (1..2).each do # name, type
        @parser.read_object_key
        @parser.read_string
      end
      @parser.read_object_key # chat_id
      @chat_id = @parser.read_int
    end

    def messages(start_after_id : Int32 | Int64, &)
      @parser.read_object_key # messages
      @parser.read_array do
        message = Message.from_json @parser.read_raw
        next if message.id <= start_after_id || message.type != "message" || !message.from_id || message.forwarded_from || message.text_entities.size == 0
        message.chat_id = @chat_id
        yield message
      end
    end
  end

  class Database
    class_property path : Path
    {% if flag?(:windows) %}
      @@path = Path.new("~", "AppData", "conticrystal", "db.db").expand(home: true)
    {% else %}
      @@path = Path.new("~", ".config", "conticrystal", "db.db").expand(home: true)
    {% end %}

    @db : DB::Database

    def initialize
      Dir.mkdir_p @@path.parent
      @db = DB.open "sqlite3://#{@@path}"
      @db.exec "pragma synchronous=off"
      @db.exec "pragma locking_mode=exclusive"
      @db.exec "pragma journal_mode=memory"
      @db.exec "create table if not exists messages(" \
               "chat_id int not null," \
               "message_id int not null," \
               "from_id int not null," \
               "unique(chat_id, message_id))"
      @db.exec "create index if not exists messages_from_id on messages(from_id)"

      @db.exec "create table if not exists words(value text unique not null)"
      @db.exec "create unique index if not exists words_value on words(value)"
      @db.exec "insert or ignore into words (value) values (?)", "."

      @db.exec "create table if not exists transitions(" \
               "current_word int not null," \
               "next_word int not null," \
               "foreign key(current_word) references words(rowid)," \
               "foreign key(next_word) references words(rowid))"
      @db.exec "create index if not exists transitions_current_word on transitions(current_word)"

      @db.exec "create table if not exists transitions_messages(" \
               "transition int not null," \
               "message int not null," \
               "foreign key(transition) references transition(rowid)," \
               "foreign key(message) references messages(rowid)," \
               "unique(transition, message))"

      @db.exec "create index if not exists transitions_messages_transition on transitions_messages(transition)"
    end

    def last_message_id(chat_id : Int64)
      result = @db.scalar("select max(message_id) from messages where chat_id=?", chat_id)
      result ? result.as(Int64) : 0
    end

    def <<(message : Message)
      message_insert_result = @db.exec("insert or ignore into messages (chat_id, message_id, from_id) values (?, ?, ?)", message.chat_id, message.id, message.from_id)
      return if message_insert_result.rows_affected == 0 # message already exists
      m_rowid = message_insert_result.last_insert_id
      prev = "."
      message.words do |cur|
        @db.exec "insert or ignore into words (value) values (?)", cur
        t_rowid = @db.exec("insert into transitions (current_word, next_word)" \
                           "select c.rowid, n.rowid from words as c join words as n " \
                           "on c.value == ? and n.value == ?", prev, cur).last_insert_id
        @db.exec "insert or ignore into transitions_messages (transition, message) values (?, ?)", t_rowid, m_rowid
      end
    end
  end
end

database = Conticrystal::Database.new

dump = Conticrystal::Dump.new Path.new "/home/necheporenko_s_iu/repositories/dump.json"
dump.messages(database.last_message_id(dump.chat_id)) do |message|
  database << message
end
