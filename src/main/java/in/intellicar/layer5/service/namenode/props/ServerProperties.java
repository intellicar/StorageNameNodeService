package in.intellicar.layer5.service.namenode.props;

import com.fasterxml.jackson.databind.JsonNode;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.MySQLProps;
import in.intellicar.layer5.beacon.storagemetacls.service.common.props.NettyProps;
import in.intellicar.layer5.utils.JsonUtils;
import in.intellicar.layer5.utils.PathUtils;

import java.util.logging.Logger;

/**
 * @author : naveen
 * @since : 02/03/21, Tue
 */


public class ServerProperties {
    public String configFile;
    public boolean isValid;

    public static String APPID_TAG = "appid";
    public String appId;

    public static String SCRATCHDIR_TAG = "scratchdir";
    public String scratchDir;

    public static String STOPFILE_TAG = "stopfile";
    public String stopFile;

    public static String NETTY_TAG = "netty";
    public NettyProps nettyProps;

    public static String DB_TAG = "db";
    public JsonNode dbProps;
    public static String MYSQLDB_TAG = "mysql";
    public MySQLProps dbMySQLProps;

    public String[] mustTags = {APPID_TAG, SCRATCHDIR_TAG, STOPFILE_TAG, NETTY_TAG, DB_TAG};

    public ServerProperties(String pathFile, Logger logger) {
        this.configFile = pathFile;
        isValid = false;
        if (PathUtils.fileExists(pathFile, logger))
            initProperties(logger);
    }

    public boolean initProperties(Logger logger) {
        try {
            JsonNode configJson = JsonUtils.parseJson(configFile, logger);
            if (configJson == null || !configJson.isObject())
                return false;

            if (!JsonUtils.hasFields(configJson, mustTags)) {
                logger.info("Make sure the required fields are present");
                return false;
            }

            appId = configJson.get(APPID_TAG).asText();
            scratchDir = configJson.get(SCRATCHDIR_TAG).asText();
            stopFile = configJson.get(STOPFILE_TAG).asText();

            nettyProps = NettyProps.parseJson(configJson.get(NETTY_TAG), logger);
            if (nettyProps == null)
                return false;

            dbProps = configJson.get(DB_TAG);
            if (!(dbProps.isObject() && dbProps.has(MYSQLDB_TAG)))
                return false;
            MySQLProps mySQLProps = MySQLProps.parseMySQLConfig(dbProps.get(MYSQLDB_TAG), logger);
            if (mySQLProps == null)
                return false;
            dbMySQLProps = mySQLProps;

            isValid = true;
            return true;
        } catch (Exception e) {
            isValid = false;
            return false;
        }
    }

    public void printDebug(Logger logger) {
        logger.info("App id:" + appId);
        logger.info("scratchDir:" + scratchDir);
        logger.info("stopFile:" + stopFile);
        logger.info("nettyProps:" + JsonUtils.pojoToJson(nettyProps, logger));
        logger.info("Mysql Props:" + JsonUtils.pojoToJson(dbMySQLProps, logger));
    }
}