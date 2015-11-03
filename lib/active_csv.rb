# coding: utf-8
#
# Description:
#   CSVのデータをもとにモデルから参照するときに1件づつ参照すると時間がかかる
#   しかし、全件まとめてモデルのインスタンスを生成するとメモリ消費が不安
#   そこでCSVからの取得を指定件数づつ行うためのライブラリ
# 
# Example:
#   # batch_sizeにまとめて取得する件数を指定
#   ActiveCSV.bulk_foreach('test.csv', encoding: 'UTF-8', batch_size: 5) do |rows|
#     # 指定したカラム番号のデータを配列で取得
#     ids = rows.values(0)
#
#     # 例えばこんな感じに数件づつモデルから取得するのに利用
#     SampleModel.where(id: ids).each do |model|
#       # さらに処理中のデータに対応させることも可能
#       row = rows.find_by_value(0, model.id)
#     end
#   end

require "csv"

class ActiveCSV < CSV
  DEFAULT_BATCH_SIZE = 20

  class << self
    def bulk_foreach(path, options = {})
      batch_size = options.delete(:batch_size) || DEFAULT_BATCH_SIZE
      
      # ファイル自体は一度全てオンメモリに読み込まれる点に注意
      # 大きいファイルを扱うことも考えるならファイル自体を行単位で読み込むようにする
      read(path, options).each_slice(batch_size) do |rows|
        yield(Rows.new(rows))
      end
    end
  end

  class Rows
    include Enumerable

    attr_reader :rows

    def initialize(rows)
      @rows = rows
    end

    def [](key)
      @rows[key]
    end

    def each
      @rows.each { |row| yield(row) }
    end

    def values(column_num)
      rows.map { |row| row[column_num] }
    end

    def find_by_value(column_num, value)
      rows.find { |row| row[column_num] == value } || []
    end

    def size
      rows.size
    end
  end
end
