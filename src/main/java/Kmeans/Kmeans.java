package Kmeans; /**
 * Created by Qiang on 2016/12/23.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

public class Kmeans {
    /*
    * 实现k-means聚类方法
    */
    public  List<Cluster> kmeansFunc(List<UserModelXY> list, Integer k){
        List<Cluster> kCluster = new LinkedList<Cluster>();
        for(int i = 0; i < k; i++){   //随机根据前k条数据的(x, y)坐标作为簇的质心
            Cluster cl = new Cluster(list.get(i));
            cl.setClusterid(i);
            kCluster.add(cl);
        }
        operateFunc(list, kCluster, k);
//        for(int i = 0 ; i< k ; i++){
//            System.out.println("这是第"+ kCluster.get(i).getClusterid() +"类,一共有" + kCluster.get(i).getListPoint().size());
//            Cluster c = kCluster.get(i);
//            for(UserModelXY p : c.getListPoint()){
//                System.out.println("用户"+ p.getId() + "("+
//                        p.getAction() + "," +
//                        p.getAdventure() + ","+
//                        p.getAnimation() + ","+
//                        p.getChildren() + ","+
//                        p.getComedy() + ","+
//                        p.getCrime() + ","+
//                        p.getDocumentary() + ","+
//                        p.getDrama() + ","+
//                        p.getFantasy() + ","+
//                        p.getFilmnoir()+ ","+
//                        p.getHorror() + ","+
//                        p.getMusical()+ ","+
//                        p.getMystery()+ ","+
//                        p.getRomance()+ ","+
//                        p.getScifi()+ ","+
//                        p.getThriller()+ ","+
//                        p.getWar()+ ","+
//                        p.getWestern()+
//                        ") 属于 第：" + c.getClusterid() + "类 ");
//            }
//        }
//
        return kCluster;
    }

    /*
      * 具体的聚类操作方法
      * @ list 原始数据
      * @ kCluster 生成的k个簇
      * @ k  聚类个数
      */
    public void operateFunc(List<UserModelXY> list, List<Cluster> kCluster, int k){
        for(Cluster c: kCluster){
            if(c.getListPoint() != null){
                c.getListPoint().clear();
            }
        }
        for(UserModelXY p: list){
            double distance = 100000.0;
            double tempDistance = 0.0;
            int centerid = kCluster.get(0).getClusterid();
            for(Cluster c: kCluster){
                if(distance > (tempDistance = calDistance(p, c))){
                    centerid = c.getClusterid();
                    distance = tempDistance;
                }
            }
            kCluster.get(centerid).addPoint(p);  //将该点添加到离kCluster集合 最近质心的簇的listPoint中去  在后面的显示和计算新的质心用到listPoint
        }

        for(Cluster c: kCluster){ //计算新的质心
            c.calCenterid();
        }

        boolean flag = true;    //用来作为判断新旧质心是否相同的标志
        for(Cluster c: kCluster){  //判断新旧质心是否相同作为聚类结束的标准
            if(c.getNewCenterid().getAction() != c.getPreCenterid().getAction()
                    || c.getNewCenterid().getAdventure() != c.getPreCenterid().getAdventure()
                    || c.getNewCenterid().getAnimation() != c.getPreCenterid().getAnimation()
                    || c.getNewCenterid().getChildren() != c.getPreCenterid().getChildren()
                    || c.getNewCenterid().getComedy() != c.getPreCenterid().getComedy()
                    || c.getNewCenterid().getCrime() != c.getPreCenterid().getCrime()
                    || c.getNewCenterid().getDocumentary() != c.getPreCenterid().getDocumentary()
                    || c.getNewCenterid().getDrama() != c.getPreCenterid().getDrama()
                    || c.getNewCenterid().getFantasy() != c.getPreCenterid().getFantasy()
                    || c.getNewCenterid().getFilmnoir() != c.getPreCenterid().getFilmnoir()
                    || c.getNewCenterid().getHorror() != c.getPreCenterid().getHorror()
                    || c.getNewCenterid().getMusical() != c.getPreCenterid().getMusical()
                    || c.getNewCenterid().getMystery() != c.getPreCenterid().getMystery()
                    || c.getNewCenterid().getRomance() != c.getPreCenterid().getRomance()
                    || c.getNewCenterid().getScifi() != c.getPreCenterid().getScifi()
                    || c.getNewCenterid().getThriller() != c.getPreCenterid().getThriller()
                    || c.getNewCenterid().getWar() != c.getPreCenterid().getWar()
                    || c.getNewCenterid().getWestern() != c.getPreCenterid().getWestern()
                    ){
                c.setPreCenterid(c.getNewCenterid());
                flag = false;
            }
        }

        if(!flag){
            operateFunc(list, kCluster, k);
        }
        return;
    }




    /*
   * 计算测试记录和已划分记录的距离
   */
    public Double calDistance(UserModelXY p, Cluster c){
        Double resDistance = 0.0;
        double action = p.getAction() - c.getPreCenterid().getAction();
        double adventure = -c.getPreCenterid().getAdventure() + p.getAdventure();
        double animation = -c.getPreCenterid().getAnimation() + p.getAnimation();
        double children = -c.getPreCenterid().getChildren() + p.getChildren();
        double comedy = -c.getPreCenterid().getComedy() + p.getComedy();
        double crime = -c.getPreCenterid().getCrime() + p.getCrime();
        double documentary = -c.getPreCenterid().getDocumentary() + p.getDocumentary();
        double drama = -c.getPreCenterid().getDrama() + p.getDrama();
        double fantasy = -c.getPreCenterid().getFantasy() + p.getFantasy();
        double filmnoir = -c.getPreCenterid().getFilmnoir() + p.getFilmnoir();
        double horror = -c.getPreCenterid().getHorror() + p.getHorror();
        double musical = -c.getPreCenterid().getMusical() + p.getMusical();
        double mystery = -c.getPreCenterid().getMystery() + p.getMystery();
        double romance = -c.getPreCenterid().getRomance() + p.getRomance();
        double scifi = -c.getPreCenterid().getScifi() + p.getScifi();
        double thriller = -c.getPreCenterid().getThriller() + p.getThriller();
        double war = -c.getPreCenterid().getWar() + p.getWar();
        double western = -c.getPreCenterid().getWestern() + p.getWestern();
        resDistance = Math.sqrt(action * action + adventure * adventure + animation * animation+ children * children+ comedy * comedy+ crime * crime
                + documentary * documentary+ drama * drama + fantasy * fantasy+ filmnoir * filmnoir+ horror * horror+ musical * musical
                + mystery * mystery+ romance * romance+ scifi * scifi+ thriller * thriller+ war * war+ western * western);  //欧氏距离比较
        return resDistance;
    }

}
