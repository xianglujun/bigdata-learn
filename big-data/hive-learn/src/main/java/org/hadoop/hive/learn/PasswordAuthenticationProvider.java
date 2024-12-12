package org.hadoop.hive.learn;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class PasswordAuthenticationProvider implements PasswdAuthenticationProvider {

    private final HiveConf hiveConf;
    private final static String DEFAULT_PASSWD_PATH = "/opt/apps/passwd";
    private final String filePath;

    private static final Logger LOG = LoggerFactory.getLogger(PasswordAuthenticationProvider.class);

    private Map<String, String> userPwdMap = new HashMap<String, String>();

    public PasswordAuthenticationProvider(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
        String filePath = this.hiveConf.get("hive.server2.custom.authentication.file");
        this.filePath = filePath == null ? DEFAULT_PASSWD_PATH : filePath;

        init();
    }

    private void init() {
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line = "";
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                String[] splits = line.split(":");
                userPwdMap.put(splits[0], splits[1]);
            }
        } catch (Exception e) {
            LOG.error("初始化失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

       LOG.info("数据初始化完成");
    }



    @Override
    public void Authenticate(String s, String s1) throws AuthenticationException {
        String pwd = userPwdMap.get(s);
        if (pwd != null && !"".equals(s1.trim())) {
            return;
        }

        throw new AuthenticationException("用户名或密码错误");
    }
}
