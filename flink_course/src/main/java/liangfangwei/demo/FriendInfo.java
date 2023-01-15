package liangfangwei.demo;

import lombok.Data;

import java.io.Serializable;

@Data
class FriendInfo implements Serializable {
    private int fid;
    private String name;
}

