package jp.gr.java_conf.shindooo.peppyyammeranalyzer.entity;

/**
 * 送信者とハイテンションスコアの組
 */
public class SenderPeppyScore {

    /**
     * 送信者ID
     */
    public Long senderId;

    /**
     * ハイテンションスコア（ハイテンション文字の数）
     */
    public Integer peppyScore;

    /**
     * 送信者名
     */
    public String senderName;

    /**
     * 送信者名を除いて初期化するコンストラクタ
     *
     * @param senderId 送信者ID
     * @param peppyScore ハイテンションスコア
     */
    public SenderPeppyScore(Long senderId, Integer peppyScore) {
        this.senderId = senderId;
        this.peppyScore = peppyScore;
    }

    /**
     * すべてのフィールドを初期化するコンストラクタ
     *
     * @param senderId 送信者ID
     * @param peppyScore ハイテンションスコア
     * @param senderName 送信者名
     */
    public SenderPeppyScore(Long senderId, Integer peppyScore, String senderName) {
        this.senderId = senderId;
        this.peppyScore = peppyScore;
        this.senderName = senderName;
    }

}
