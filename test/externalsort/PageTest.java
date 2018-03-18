package externalsort;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class PageTest {
    private int[] testData = new int[] { 5, 8, 333, 2, 999, 16, -55 };
    private Page page;

    @BeforeEach
    void setUp() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
        IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(testData);
        this.page = new Page(byteBuffer.array(), testData.length * Page.FIELD_SIZE);
    }

    @AfterEach
    void tearDown() {
        this.page = null;
    }

    @Test
    void pageIteratorTest() {
        int fieldCount = 0;
        Iterator<Integer> pageIterator1 = page.iterator();
        while (pageIterator1.hasNext()) {
            pageIterator1.next();
            fieldCount++;
        }
        assertEquals(fieldCount, testData.length);

        int fieldIdx = 0;
        Iterator<Integer> pageIterator2 = page.iterator();
        while (pageIterator2.hasNext()) {
            assertEquals(testData[fieldIdx], (int)pageIterator2.next());
            fieldIdx++;
        }
    }

    @Test
    void sortTest() {
        page.sort();
        Iterator<Integer> pageIterator = page.iterator();
        int previousValue = Integer.MIN_VALUE;
        while (pageIterator.hasNext()) {
            int currentValue = pageIterator.next();
            assertTrue(
                currentValue >= previousValue,
                "Previous value is not less then or equal to the current value"
            );
            previousValue = currentValue;
        }
    }

    @Test
    void serializeTest() throws IOException {
        byte[] serializedPage = page.serialize();

        Page pageFromSerializedData = new Page(serializedPage, testData.length * Page.FIELD_SIZE);

        Iterator<Integer> iterator1 = page.iterator();
        Iterator<Integer> iterator2 = pageFromSerializedData.iterator();

        while (iterator1.hasNext() && iterator2.hasNext()) {
            assertEquals(iterator1.next(), iterator2.next());
        }
    }

    @Test
    void addFieldTest() {
        Page newPage = new Page();
        int maxNumFields = Page.PAGE_SIZE / Page.FIELD_SIZE;

        for (int i = 0; i < maxNumFields; i++) {
            try {
                newPage.addField(i);
            } catch (Exception e) {
                fail("Page should not be full");
            }
        }

        Iterator<Integer> pageIterator = newPage.iterator();

        for (int i = 0; i < maxNumFields; i++) {
            assertEquals(i, (int) pageIterator.next());
        }

        assertFalse(pageIterator.hasNext());

        assertThrows(PageFullException.class, () -> newPage.addField(999));

    }
}