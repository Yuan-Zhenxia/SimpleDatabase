package simpleDatabase.basic;

import simpleDatabase.cache.DbFile;
import simpleDatabase.cache.HeapFile;
import simpleDatabase.cache.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * finished on 29 Jul.
 *
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    // DbFile 包含很多个HeapPage，HeapPage 4kb 包含多个 Tuples

    private HashMap<Integer /** tableId **/, DbFile /** DbFiles **/ > id2file;

    private HashMap<Integer /** tableId **/, String /** primary key **/> id2priKey;

    private HashMap<Integer /** tableId **/, String /** table name **/> id2name;

    private HashMap<String /** table name **/, Integer /** tableId **/> name2id;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        id2file = new HashMap<>();
        id2priKey = new HashMap<>();
        id2name = new HashMap<>();
        name2id = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        if (name == null || pkeyField == null) throw new IllegalArgumentException();

        int tableId = file.getId();
        if (name2id.containsKey(name)) {
            // 表已经存在
            throw new UnsupportedOperationException("The table is already exists");
        }
        // 插入
        id2file.put(tableId, file);
        id2name.put(tableId, name);
        id2priKey.put(tableId, pkeyField);
        name2id.put(name, tableId);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if (name == null || !name2id.containsKey(name)) throw new NoSuchElementException();
        return name2id.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        if (!isIdValid(tableid, id2file)) throw new NoSuchElementException();
        return id2file.get(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        if (!isIdValid(tableid, id2file)) throw new NoSuchElementException();
        return id2file.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if (!isIdValid(tableid, id2priKey)) throw new NoSuchElementException();
        return id2priKey.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return id2name.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        if (!isIdValid(id, id2name)) throw new NoSuchElementException();
        return id2name.get(id);
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        id2name.clear();
        id2priKey.clear();
        id2file.clear();
        name2id.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                // TODO: 按照这句话的意思，表格的数据.dat文件必须是放在与catalog文件的同一个文件夹下
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }

    private boolean isIdValid(int id, HashMap<?, ?> map) {
        return map.containsKey(id);
    }
}

