use async_trait::async_trait;
use derive_new::new;
use kernel::model::{
    book::{event::CreateBook, Book},
    id::BookId,
};
use kernel::repository::book::BookRepository;
use shared::error::{AppError, AppResult};

use crate::database::ConnectionPool;
use crate::database::model::book::BookRow;

#[derive(new)]
pub struct BookRepositoryImpl {
    db: ConnectionPool,
}

#[async_trait]
impl BookRepository for BookRepositoryImpl {
    async fn create(&self, event: CreateBook) -> AppResult<()> {
        sqlx::query!(
            r#"
                INSERT INTO books (title, author, isbn, description)
                VALUES($1, $2, $3, $4)
            "#,
            event.title,
            event.author,
            event.isbn,
            event.description
        )
        .execute(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;

        Ok(())
    }

    async fn find_all(&self) -> AppResult<Vec<Book>> {
        let rows: Vec<BookRow> = sqlx::query_as!(
            BookRow,
            r#"
                SELECT
                    book_id,
                    title,
                    author,
                    isbn,
                    description
                FROM books
                ORDER BY created_at DESC
            "#
        )
        .fetch_all(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;

        Ok(rows.into_iter().map(Book::from).collect())
    }

    async fn find_by_id(&self, book_id: BookId) -> AppResult<Option<Book>> {
        let rows: Option<BookRow> = sqlx::query_as!(
            BookRow,
            r#"
                SELECT
                    book_id,
                    title,
                    author,
                    isbn,
                    description
                FROM books
                WHERE book_id = $1
            "#,
            book_id as _ // query_as!マクロのコンパイルによる型チェックを無効化
        )
        .fetch_optional(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;

        Ok(rows.map(Book::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test]
    async fn test_register_book(pool: sqlx::PgPool) -> anyhow::Result<()>{
        // BookRepositoryImplを初期化
        let repo = BookRepositoryImpl::new(ConnectionPool::new(pool));

        // 投入するための蔵書データを作成
        let book = CreateBook {
            title: "Test Title".into(),
            author: "Test Author".into(),
            isbn: "Test ISBN".into(),
            description: "Test Description".into(),
        };

        // 蔵書データを投入すると正常終了することを確認
        repo.create(book).await?;

        // 蔵書の一覧を取得すると投入した1件だけ取得することを確認
        let res = repo.find_all().await?;
        assert_eq!(res.len(), 1);

        // 蔵書の一覧の最初のデータから蔵書IDを取得し、
        // find_by_idメソッドでその蔵書データを取得できることを確認
        let book_id = res[0].id;
        let res = repo.find_by_id(book_id).await?;
        assert!(res.is_some());

        // 取得した蔵書データが CreateBook で投入した
        // 蔵書データと一致することを確認
        let Book { id, title, author, isbn, description } = res.unwrap();
        assert_eq!(id, book_id);
        assert_eq!(title, "Test Title");
        assert_eq!(author, "Test Author");
        assert_eq!(isbn, "Test ISBN");
        assert_eq!(description, "Test Description");

        Ok(())
    }
}