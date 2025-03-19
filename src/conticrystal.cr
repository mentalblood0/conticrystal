require "http/client"
require "yaml"
require "json"
require "sqlite3"

module Conticrystal
  struct Message
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
    class_getter dir : Path
    {% if flag?(:windows) %}
      @@dir = Path.new("~", "AppData", "conticrystal").expand(home: true)
    {% else %}
      @@dir = Path.new("~", ".local", "share", "conticrystal").expand(home: true)
    {% end %}

    getter chat_id : Int64

    def initialize(@path : Path)
      Dir.mkdir_p @@dir
      @parser = JSON::PullParser.new File.new @path
      @parser.read_begin_object
      (1..2).each do # name, type
        @parser.read_object_key
        @parser.read_string
      end
      @parser.read_object_key # chat_id
      @chat_id = @parser.read_int
    end

    def messages(start_after : Int32 | Int64, &)
      @parser.read_object_key # messages
      @parser.read_array do
        message = Message.from_json @parser.read_raw
        next if message.id <= start_after
        next if message.type != "message"
        next if !message.from_id
        next if !message.from_id.not_nil!.starts_with? "user"
        next if message.forwarded_from
        next if message.text_entities.size == 0
        message.chat_id = @chat_id
        yield message
      end
    end

    def mark_processed
      new_path = @path.parent / "processed" / @path.basename
      Dir.mkdir_p new_path.parent
      File.rename @path, new_path
      @path = new_path
    end

    def self.unprocessed(&)
      Dir.glob @@dir / "*.json" do |path|
        yield Dump.new Path.new path
      end
    end
  end

  class Database
    class_getter dir : Path
    {% if flag?(:windows) %}
      @@dir = Path.new("~", "AppData", "conticrystal").expand(home: true)
    {% else %}
      @@dir = Path.new("~", ".local", "share", "conticrystal").expand(home: true)
    {% end %}

    @db : DB::Database

    def initialize(user_id : String)
      Dir.mkdir_p @@dir
      path = @@dir / "#{user_id}.db"
      @db = DB.open "sqlite3://#{path}"
      @db.exec "pragma synchronous=off"
      @db.exec "pragma locking_mode=exclusive"
      @db.exec "pragma journal_mode=memory"
      @db.exec "create table if not exists messages(" \
               "chat_id int not null," \
               "message_id int not null," \
               "unique(chat_id, message_id))"

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

    def <<(message : Message)
      message_insert_result = @db.exec("insert or ignore into messages (chat_id, message_id) values (?, ?)", message.chat_id, message.id)
      return if message_insert_result.rows_affected == 0 # message already exists
      prev = "."
      message.words do |cur|
        @db.exec "insert or ignore into words (value) values (?)", cur
        t_rowid = @db.exec("insert into transitions (current_word, next_word)" \
                           "select c.rowid, n.rowid from words as c join words as n " \
                           "on c.value == ? and n.value == ?", prev, cur).last_insert_id
        @db.exec "insert or ignore into transitions_messages (transition, message) values (?, ?)", t_rowid, message_insert_result.last_insert_id
        prev = cur
      end
    end

    def generate(prev : String)
      @db.query_one? "select wn.value, m.chat_id, m.message_id from words as wc join transitions as t " \
                     "join words as wn join transitions_messages as tm join messages as m " \
                     "on wc.value=? and t.current_word=wc.rowid and " \
                     "wn.rowid=t.next_word and tm.transition=t.rowid and m.rowid=tm.message " \
                     "order by random() limit 1", prev, as: {word: String, chat_id: Int64, message_id: Int64}
    end

    def self.exists?
      Dir.glob(@@dir / "*.db").size > 0
    end

    def self.random
      Database.new Path.new(Dir.glob(@@dir / "*.db").sample).stem
    end

    def close
      @db.close
    end
  end

  class Versions
    class_getter path : Path
    {% if flag?(:windows) %}
      @@path = Path.new("~", "AppData", "conticrystal", "update.lock").expand(home: true)
    {% else %}
      @@path = Path.new("~", ".local", "share", "conticrystal", "update.lock").expand(home: true)
    {% end %}

    include YAML::Serializable

    @chats = {} of Int64 => Int32 | Int64 # chat id => last processed message id

    def initialize
    end

    def self.load
      Versions.from_yaml File.new Versions.path rescue Versions.new
    end

    def [](chat_id : Int64)
      @chats[chat_id]? || 0
    end

    def <<(message : Message)
      @chats[message.chat_id] = message.id
    end

    def save
      Dir.mkdir_p @@path.parent
      File.write @@path, self.to_yaml
    end
  end

  class Config
    {% if flag?(:windows) %}
      @@path = Path.new("~", "AppData", "conticrystal", "config.yml").expand(home: true)
    {% else %}
      @@path = Path.new("~", ".config", "conticrystal", "config.yml").expand(home: true)
    {% end %}

    class Generation
      enum Type
        ANY
        CHOOSE
        MIX
        CHOOSIX
      end

      include YAML::Serializable

      getter type : Type
      getter users = [] of String
      getter amount : Int64
    end

    class Sending
      include YAML::Serializable

      getter token : String
      getter chat_id : String
    end

    include YAML::Serializable

    getter generate : Generation
    getter send : Sending

    def self.load
      if !File.exists? @@path
        Dir.mkdir_p @@path.parent
        File.write @@path, "---
generate:
  type: ANY # ANY or CHOOSE or MIX or CHOOSIX
            # ANY means one random user
            # CHOOSE means one random user from provided
            # MIX means random user from provided, for each word
            # CHOOSIX means random user from two randomly chosen, for each word
            # for CHOOSE and MIX also provide 'users' key with users ids list (user id example is user123456789)
  amount: 100
send:
  token: # telegram bot token
  chat_id: # telegram chat id, e.g. \"-1234567890123\""
        puts "Created default config at #{@@path}"
      end
      Config.from_yaml File.new @@path
    end
  end

  class App
    @databases = {} of String => Database
    @versions : Versions = Versions.load
    @config : Config = Config.load

    def load
      Dump.unprocessed do |dump|
        dump.messages @versions[dump.chat_id] do |message|
          user_id = message.from_id.not_nil!
          database = begin
            if !@databases.has_key? user_id
              begin
                @databases[user_id] = Database.new(user_id)
              rescue # too many file descriptors used => clear cache
                @databases.each_value &.close
                @databases.clear
                @databases[user_id] = Database.new(user_id)
              end
            end
            @databases[user_id]
          end
          database << message
          @versions << message
        end
        @versions.save
        dump.mark_processed
      end
      @databases.each_value &.close
      @databases.clear
    end

    def generate(databases : Array(Database))
      end_len = 0
      result = String.build do |sb|
        prev = "."
        i = 0
        loop do
          if !(row = databases.sample.generate prev)
            prev = "."
            sb << "\\." if i > 0
            end_len = sb.bytesize
            next
          end
          prev = row[:word]

          escaped = [".", "-", "!"].includes?(prev) ? "\\#{prev}" : prev

          if [".", "!", "?", ",", ";", ":", "-", "—"].includes? prev
            sb << " " if prev == "—" || prev == "-"
            sb << escaped
          else
            sb << " " if i > 0
            sb << "[#{escaped}](https://t.me/c/#{row[:chat_id]}/#{row[:message_id]})"

            i += 1
            break if i == @config.generate.amount
          end

          end_len = sb.bytesize if [".", "!", "?"].includes? prev
        end
      end
      String.new result.to_slice[0, end_len]
    end

    def run
      load

      if !Database.exists?
        Dir.mkdir_p Database.dir
        puts "Put JSON files from telegram messages dumps at #{Database.dir} and try again, processed files will be moved to #{Database.dir / "processed"}"
        exit 1
      end

      text = case @config.generate.type
             when Config::Generation::Type::ANY
               generate [Database.random]
             when Config::Generation::Type::CHOOSE
               generate [Database.new @config.generate.users.sample]
             when Config::Generation::Type::MIX
               generate @config.generate.users.map { |user_id| Database.new user_id }
             when Config::Generation::Type::CHOOSIX
               generate (0..1).map { @config.generate.users.sample }.map { |user_id| Database.new user_id }
             else
               raise "don't know how to handle #{@config.generate.type}"
             end

      io = IO::Memory.new
      builder = HTTP::FormData::Builder.new io
      builder.field "chat_id", @config.send.chat_id
      builder.field "text", text
      builder.field "parse_mode", "MarkdownV2"
      builder.finish
      body = io.to_s
      headers = HTTP::Headers{"Content-Type" => builder.content_type}

      loop do
        response = HTTP::Client.post "https://api.telegram.org/bot#{@config.send.token}/sendMessage", headers: headers, body: body
        break if response.success?
        sleep 1.seconds
      end
    end
  end
end

Conticrystal::App.new.run
