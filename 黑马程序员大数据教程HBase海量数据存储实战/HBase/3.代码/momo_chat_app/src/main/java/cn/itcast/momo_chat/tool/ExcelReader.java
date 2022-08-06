package cn.itcast.momo_chat.tool;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.util.*;
import java.util.logging.Logger;

public class ExcelReader {
    private static Logger log = Logger.getLogger("client");

    public static void main(String[] args) {
        String xlxsPath = "D:\\课程研发\\51.V8.0_NoSQL_MQ\\2.HBase\\3.代码\\momo_chat_app\\data\\测试数据集.xlsx";
        Map<String, List<String>> mapData = readXlsx(xlxsPath, "陌陌数据");

        for(int i = 0; i < 10000; ++i) {
            System.out.println(randomColumn(mapData, "sender_nickyname"));
        }
    }

    /**
     * 随机获取某一列的数据
     * @param columnName 列名
     * @return 随机数据
     */
    public static String randomColumn(Map<String, List<String>> resultMap, String columnName) {
        List<String> valList = resultMap.get(columnName);

        if(valList == null) throw new RuntimeException("未读取到列名为" + columnName + "的任何数据!");
        Random random = new Random();

        int randomIndex = random.nextInt(valList.size());

        return valList.get(randomIndex);
    }

    /**
     * 将Excel文件读取为Map结构: <column_name, list>
     * 其中column_name为第4行的名字
     * @param path Excel文件路径（要求Excel为2007）
     * @param sheetName 工作簿名称
     * @return Map结构
     */
    public static Map<String, List<String>> readXlsx(String path, String sheetName)
    {
        // 列的数量
        int columnNum = 0;
        HashMap<String, List<String>> resultMap = new HashMap<String, List<String>>();
        ArrayList<String> columnList = new ArrayList<String>();

        try
        {
            OPCPackage pkg= OPCPackage.open(path);
            XSSFWorkbook excel=new XSSFWorkbook(pkg);
            //获取sheet
            XSSFSheet sheet=excel.getSheet(sheetName);

            // 加载列名
            XSSFRow columnRow = sheet.getRow(3);
            if(columnRow == null) {
                throw new RuntimeException("数据文件读取错误!请确保第4行为英文列名!");
            }
            else {
                Iterator<Cell> colIter = columnRow.iterator();
                // 迭代所有列
                while(colIter.hasNext()) {
                    Cell cell = colIter.next();
                    String colName = cell.getStringCellValue();
                    columnList.add(colName);
                    columnNum++;
                }
            }

            System.out.println("读取到:" + columnNum + "列");
            System.out.println(Arrays.toString(columnList.toArray()));

            // 初始化resultMap
            for(String colName : columnList) {
                resultMap.put(colName, new ArrayList<String>());
            }

            // 迭代sheet
            Iterator<Row> iter = sheet.iterator();
            int i = 0;
            int rownum = 1;

            while(iter.hasNext()) {
                Row row = iter.next();
                Iterator<Cell> cellIter = row.cellIterator();

                // 跳过前4行
                if(rownum <= 4) {
                    ++rownum;
                    continue;
                }

                while(cellIter.hasNext()) {
                    XSSFCell cell=(XSSFCell) cellIter.next();
                    //根据单元的的类型,读取相应的结果
                    if(cell.getCellType() == CellType.NUMERIC) {
                        resultMap.get(columnList.get(i % columnList.size())).add(Double.toString(cell.getNumericCellValue()));
                    }
                    else {
                        resultMap.get(columnList.get(i % columnList.size())).add(cell.getStringCellValue());
                    }

                    ++i;
                    ++rownum;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return resultMap;
    }
}

