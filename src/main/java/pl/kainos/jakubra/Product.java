package pl.kainos.jakubra;

public class Product {
    private int id;
    private String productName;
    private String categoryName;
    private String productPrice;

    public Product(int id, String productName, String categoryName, String productPrice) {
        this.id = id;
        this.productName = productName;
        this.categoryName = categoryName;
        this.productPrice = productPrice;
    }

    public int getId() {
        return id;
    }

    public String getProductName() {
        return productName;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public String getProductPrice() {
        return productPrice;
    }
}
