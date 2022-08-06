package cn.itcast.momo_chat.entity;

/**
 * 陌陌打招呼消息实体类
 */
public class Msg {
    private String msg_time;                // 消息时间
    private String sender_nickyname;        // 发件人昵称
    private String sender_account;          // 发件人账号
    private String sender_sex;              // 发件人性别
    private String sender_ip;               // 发件人IP
    private String sender_os;               // 发件人系统
    private String sender_phone_type;       // 发件人手机型号
    private String sender_network;          // 发件人网络制式
    private String sender_gps;              // 发件人GPS
    private String receiver_nickyname;      // 收件人昵称
    private String receiver_ip;             // 收件人IP
    private String receiver_account;        // 收件人账号
    private String receiver_os;             // 收件人系统
    private String receiver_phone_type;     // 收件人手机型号
    private String receiver_network;        // 收件人网络制式
    private String receiver_gps;            // 收件人GPS
    private String receiver_sex;            // 收件人性别
    private String msg_type;                // 消息类型
    private String distance;                // 双方距离
    private String message;                 // 消息

    public String getMsg_time() {
        return msg_time;
    }

    public void setMsg_time(String msg_time) {
        this.msg_time = msg_time;
    }

    public String getSender_nickyname() {
        return sender_nickyname;
    }

    public void setSender_nickyname(String sender_nickyname) {
        this.sender_nickyname = sender_nickyname;
    }

    public String getSender_account() {
        return sender_account;
    }

    public void setSender_account(String sender_account) {
        this.sender_account = sender_account;
    }

    public String getSender_sex() {
        return sender_sex;
    }

    public void setSender_sex(String sender_sex) {
        this.sender_sex = sender_sex;
    }

    public String getSender_ip() {
        return sender_ip;
    }

    public void setSender_ip(String sender_ip) {
        this.sender_ip = sender_ip;
    }

    public String getSender_os() {
        return sender_os;
    }

    public void setSender_os(String sender_os) {
        this.sender_os = sender_os;
    }

    public String getSender_phone_type() {
        return sender_phone_type;
    }

    public void setSender_phone_type(String sender_phone_type) {
        this.sender_phone_type = sender_phone_type;
    }

    public String getSender_network() {
        return sender_network;
    }

    public void setSender_network(String sender_network) {
        this.sender_network = sender_network;
    }

    public String getSender_gps() {
        return sender_gps;
    }

    public void setSender_gps(String sender_gps) {
        this.sender_gps = sender_gps;
    }

    public String getReceiver_nickyname() {
        return receiver_nickyname;
    }

    public void setReceiver_nickyname(String receiver_nickyname) {
        this.receiver_nickyname = receiver_nickyname;
    }

    public String getReceiver_ip() {
        return receiver_ip;
    }

    public void setReceiver_ip(String receiver_ip) {
        this.receiver_ip = receiver_ip;
    }

    public String getReceiver_account() {
        return receiver_account;
    }

    public void setReceiver_account(String receiver_account) {
        this.receiver_account = receiver_account;
    }

    public String getReceiver_os() {
        return receiver_os;
    }

    public void setReceiver_os(String receiver_os) {
        this.receiver_os = receiver_os;
    }

    public String getReceiver_phone_type() {
        return receiver_phone_type;
    }

    public void setReceiver_phone_type(String receiver_phone_type) {
        this.receiver_phone_type = receiver_phone_type;
    }

    public String getReceiver_network() {
        return receiver_network;
    }

    public void setReceiver_network(String receiver_network) {
        this.receiver_network = receiver_network;
    }

    public String getReceiver_gps() {
        return receiver_gps;
    }

    public void setReceiver_gps(String receiver_gps) {
        this.receiver_gps = receiver_gps;
    }

    public String getReceiver_sex() {
        return receiver_sex;
    }

    public void setReceiver_sex(String receiver_sex) {
        this.receiver_sex = receiver_sex;
    }

    public String getMsg_type() {
        return msg_type;
    }

    public void setMsg_type(String msg_type) {
        this.msg_type = msg_type;
    }

    public String getDistance() {
        return distance;
    }

    public void setDistance(String distance) {
        this.distance = distance;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Msg{" +
                "msg_time='" + msg_time + '\'' +
                ", sender_nickyname='" + sender_nickyname + '\'' +
                ", sender_account='" + sender_account + '\'' +
                ", sender_sex='" + sender_sex + '\'' +
                ", sender_ip='" + sender_ip + '\'' +
                ", sender_os='" + sender_os + '\'' +
                ", sender_phone_type='" + sender_phone_type + '\'' +
                ", sender_network='" + sender_network + '\'' +
                ", sender_gps='" + sender_gps + '\'' +
                ", receiver_nickyname='" + receiver_nickyname + '\'' +
                ", receiver_ip='" + receiver_ip + '\'' +
                ", receiver_account='" + receiver_account + '\'' +
                ", receiver_os='" + receiver_os + '\'' +
                ", receiver_phone_type='" + receiver_phone_type + '\'' +
                ", receiver_network='" + receiver_network + '\'' +
                ", receiver_gps='" + receiver_gps + '\'' +
                ", receiver_sex='" + receiver_sex + '\'' +
                ", msg_type='" + msg_type + '\'' +
                ", distance='" + distance + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
