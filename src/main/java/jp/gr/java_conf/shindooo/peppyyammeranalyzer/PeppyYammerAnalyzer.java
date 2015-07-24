package jp.gr.java_conf.shindooo.peppyyammeranalyzer;

/* PeppyYammerAnalyzer.java */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import jp.gr.java_conf.shindooo.peppyyammeranalyzer.yammer.YammerClient;
import jp.gr.java_conf.shindooo.peppyyammeranalyzer.entity.SenderPeppyScore;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

/**
 * Apache Sparkのデモプログラム メインクラス
 *
 * @author Shindooo
 */
public class PeppyYammerAnalyzer {

    /**
     * Sparkとしてのアプリケーション名
     */
    private static final String SPARK_APP_NAME = "PeppyYammerAnalyzer";

    private static final String[] PEPPY_CHAR_REGEXPS = {"!", "！", "っ", "ッ", "ぉ", "ォ", "〜", "(.)\\1{2,}"};

    /**
     * Yammerの投稿を解析し、ハイテンションな投稿をしている送信者ベスト3をJSONに出力する
     *
     * @param args 使用しない
     * @throws IOException
     * @throws ConfigurationException
     */
    public static void main(String[] args) throws IOException, ConfigurationException {

        PropertiesConfiguration appConfig = new PropertiesConfiguration("app.properties");

        YammerClient yammerClient = new YammerClient();
        // Yammerからトピックに関するメッセージをJson形式で取得
        String messagesJson = yammerClient.getMesagesAboutTopicAsJson();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readValue(messagesJson, JsonNode.class);

        if (!rootNode.has("references") || !rootNode.has("messages")) {
            throw new IOException("Unexpected JSON Format.");
        }

        // 送信者名の名前解決に使用
        JsonNode referencesNode = rootNode.findPath("references");
        // メッセージの配列を持つノード
        JsonNode messagesNode = rootNode.findPath("messages");

        Path messagesPath = Paths.get(appConfig.getString("filepath.messages"));
        try (BufferedWriter bw = Files.newBufferedWriter(messagesPath)) {
            bw.write(messagesNode.toString());
        }

        // ☆☆Sparkによる分散処理を行う部分-Yammerのハイテンションメッセージトップ3の集計を行う
        List<SenderPeppyScore> top3SenderPeppyScoreList = aggregateUsingSpark(messagesPath);

        // 参照用データから送信者IDで送信者名を探し、リスト各要素にセットする
        top3SenderPeppyScoreList = fillSenderNameOfList(top3SenderPeppyScoreList, referencesNode);

        // 最終結果（ハイテンションな送信者ランキング）をJsonファイルに書き出す
        String rankingJson = mapper.writeValueAsString(top3SenderPeppyScoreList);
        Path resultPath = Paths.get(appConfig.getString("filepath.ranking"));
        try (BufferedWriter bw = Files.newBufferedWriter(resultPath)) {
            bw.write(rankingJson);
        }
    }

    /**
     * ☆☆Yammerのハイテンションメッセージトップ3の集計をSparkの分散処理で行う
     *
     * @param messagesJsonFilePath 
     * @return Yammerのハイテンションメッセージトップ3
     */
    public static List<SenderPeppyScore> aggregateUsingSpark(Path messagesJsonFilePath) {

        // ☆Spark設定
        SparkConf sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster("local");
        // Sparkクラスタへの接続を表す
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // ☆JsonをRDDに変換するには Spark SQL が便利
        SQLContext sqlContext = new SQLContext(sparkContext);
        DataFrame messageDataFrame = sqlContext.read().json(messagesJsonFilePath.toString());

        // ☆開発時にprintSchema()メソッドでデータ構造を出力し、処理対象列の列インデックスを確認する必要がある（列名は使えないため）
        messageDataFrame.printSchema();

        // DataFrameから必要な列のみ抽出（TupleはScala等で用いられる値の組の概念、必須ではないが、記述がすっきりする）
        JavaPairRDD<Long, String> messageRDD = messageDataFrame.javaRDD().mapToPair(row -> {

            return new Tuple2<>(row.getLong(17), row.getStruct(1).getString(1));
        });

        // ハイテンションな文字数の出現数を求める
        JavaPairRDD<Long, Integer> charCountRDD = messageRDD.mapToPair(messageTuple -> {
            String message = messageTuple._2;
            String truncatedMessage = message;

            // メッセージからハイテンションな文字を削除していく
            for (String peppyExpr : PEPPY_CHAR_REGEXPS) {
                truncatedMessage = truncatedMessage.replace(peppyExpr, "");
            }

            // ハイテンションな文字の出現数
            int peppyScore = message.length() - truncatedMessage.length();

            return new Tuple2<Long, Integer>(messageTuple._1, peppyScore);
        });

        // 送信者ごとに合算
        JavaPairRDD<Long, Integer> TotalScoreRDD = charCountRDD.reduceByKey((charCount1, charCount2) -> charCount1 + charCount2);

        // ソートおよび上位3件の抽出
        List<SenderPeppyScore> top3SenderPeppyScoreList = TotalScoreRDD.mapToPair(tuple -> tuple.swap()).sortByKey(false).take(3)
                .stream().map(tuple -> new SenderPeppyScore(tuple._2(), tuple._1)).collect(Collectors.toList());

        return top3SenderPeppyScoreList;
    }

    /**
     * 参照用データから送信者IDで送信者名を探し、各要素にセットしたリストを返す
     *
     * <p>
     * 参照用データから送信者IDで送信者名を探し、リスト各要素にセットする。<br />
     * 副作用は起こさず、新たなリストを生成して返す。
     * </p>
     *
     * @param senderPeppyScoreList 送信者/スコアオブジェクトのリスト
     * @param referencesNode 参照用データが格納されたJSONノード
     * @return 送信者名がセットされた送信者/スコアオブジェクトのリスト
     */
    private static List<SenderPeppyScore> fillSenderNameOfList(List<SenderPeppyScore> senderPeppyScoreList, JsonNode referencesNode) {

        List<SenderPeppyScore> nameFilledList
                = senderPeppyScoreList.stream().map(senderPeppyScore -> {
                    return new SenderPeppyScore(senderPeppyScore.senderId, senderPeppyScore.peppyScore, findSenderNameInReferences(senderPeppyScore.senderId, referencesNode));
                }).collect(Collectors.toList());

        return nameFilledList;
    }

    /**
     * 参照用データから送信者IDで送信者名を探す
     *
     * @param senderId 送信者ID
     * @param referencesNode 参照用データが格納されたJSONノード
     * @return 送信者名（見つからない場合は空文字）
     */
    private static String findSenderNameInReferences(Long senderId, JsonNode referencesNode) {

        for (Iterator<JsonNode> iterator = referencesNode.elements(); iterator.hasNext();) {
            JsonNode refereceEntry = iterator.next();

            if ("user".equals(refereceEntry.path("type").textValue())
                    && senderId == refereceEntry.path("id").longValue()) {

                return refereceEntry.path("full_name").textValue();
            }
        }

        return "";
    }
}
