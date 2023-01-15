package liangfangwei.demo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
  class UserInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private List<FriendInfo> friends;
}

