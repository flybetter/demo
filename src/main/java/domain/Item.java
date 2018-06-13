package domain;

import java.io.Serializable;

/**
 * Created by njuryan on 2015/12/8.
 * <p/>
 * 房源的模型
 */
public class Item implements Serializable {

     private int cityid;

     private String projectId;


    public Item(int cityid, String projectId) {
        this.cityid = cityid;
        this.projectId = projectId;
    }

    public int getCityid() {
        return cityid;
    }

    public void setCityid(int cityid) {
        this.cityid = cityid;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    @Override
    public String toString() {
        return "Item{" +
                "cityid=" + cityid +
                ", projectId='" + projectId + '\'' +
                '}';
    }
}