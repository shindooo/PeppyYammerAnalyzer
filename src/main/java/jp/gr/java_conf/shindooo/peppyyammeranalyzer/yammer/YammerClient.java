package jp.gr.java_conf.shindooo.peppyyammeranalyzer.yammer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

import com.meterware.httpunit.HttpUnitOptions;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebForm;
import com.meterware.httpunit.WebResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.http.NameValuePair;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import org.xml.sax.SAXException;

import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Yammer API からデータを取得するクラス
 */
public final class YammerClient {

    private static final String OAUTH_GET_AUTH_CODE_URL = "https://www.yammer.com/dialog/oauth?client_id=%s";
    private static final String OAUTH_ACCESS_TOKEN_URL = "https://www.yammer.com/oauth2/access_token";
    private static final String YAMMER_URL_ALL_MESSAGES = "https://www.yammer.com/api/v1/messages.json";
    private static final String YAMMER_URL_MESSAGES_ABOUT_TOPIC = "https://www.yammer.com/api/v1/messages/about_topic/%s.json";

    private static final int HTTP_STATUS_CODE_OK = 200;

    private final CloseableHttpClient httpclient;

    public YammerClient() {
        httpclient = HttpClients.createDefault();
    }

    /**
     * Yammer API からトピックに関するメッセージを取得
     *
     * @return JSON
     * @throws IOException
     * @throws ConfigurationException
     */
    public String getMesagesAboutTopicAsJson() throws IOException, ConfigurationException {

        String accessToken = getAccessToken();

        PropertiesConfiguration appConfig = new PropertiesConfiguration("app.properties");

        String topicId = appConfig.getString("yammer.topicid");
        String url = String.format(YAMMER_URL_MESSAGES_ABOUT_TOPIC, topicId);

        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Authorization", "Bearer " + accessToken);

        try (CloseableHttpResponse response = httpclient.execute(httpGet)) {

            if (HTTP_STATUS_CODE_OK == response.getStatusLine().getStatusCode()) {

                String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8.name());

                return body;

            } else {
                throw new IOException("Failed to get messages. Http status code:" + response.getStatusLine().getStatusCode());
            }
        }
    }

    /**
     * アクセストークンを取得
     *
     * @return アクセストークン
     * @throws IOException
     */
    private String getAccessToken() throws IOException, ConfigurationException {
       
        PropertiesConfiguration yammerAccount = new PropertiesConfiguration("yammeraccount.properties");

        String clientId = yammerAccount.getString("clientid");
        String email = yammerAccount.getString("email");
        String password = yammerAccount.getString("password");
        String clientSecret = yammerAccount.getString("clientsecret");

        // 認証コード取得
        String authCode = getAuthCode(clientId, email, password);

        HttpPost httpPost = new HttpPost(OAUTH_ACCESS_TOKEN_URL);

        List<NameValuePair> paramList = new ArrayList<>();
        paramList.add(new BasicNameValuePair("client_id", clientId));
        paramList.add(new BasicNameValuePair("client_secret", clientSecret));
        paramList.add(new BasicNameValuePair("code", authCode));
        httpPost.setEntity(new UrlEncodedFormEntity(paramList));

        try (CloseableHttpResponse response = httpclient.execute(httpPost)) {

            if (HTTP_STATUS_CODE_OK == response.getStatusLine().getStatusCode()) {

                String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8.name());

                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readValue(body, JsonNode.class);

                return rootNode.findPath("access_token").findPath("token").textValue();

            } else {
                throw new IOException("Failed to get auth code. Http status code:" + response.getStatusLine().getStatusCode());
            }
        }
    }

    /**
     * 認証コードを取得
     *
     * @param clientId kuraianntoID
     * @param email メールアドレス
     * @param password パスワード
     * @return 認証コード（URLのcodeパラメータにセットされて返されるもの）
     * @throws IOException
     */
    private String getAuthCode(
            final String clientId,
            final String email,
            final String password) throws IOException {

        try {
            HttpUnitOptions.setScriptingEnabled(false);
            WebConversation wc = new WebConversation();

            WebResponse resp = wc.getResponse(String.format(OAUTH_GET_AUTH_CODE_URL, clientId));
            WebForm form = findLoginForm(resp.getForms());
            form.setParameter("login", email);
            form.setParameter("password", password);
            resp = form.submit();

            if (!resp.getURL().toString().contains("code=")) {
                resp = resp.getLinkWith("Allow").click();
            }

            return resp.getURL().toString().split("code=")[1];

        } catch (SAXException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * フォームの配列からログインフォームを見つけて返す
     *
     * @param forms WebFormの配列
     * @return ログインフォーム
     * @throws IOException
     */
    private WebForm findLoginForm(WebForm[] forms) throws IOException {
      
        Optional<WebForm> loginFormOpt = Arrays.stream(forms).filter(form -> "login-form".equalsIgnoreCase(form.getID())).findFirst();

        return loginFormOpt.orElseThrow(() -> new IOException("Login form is not found."));
    }
}
