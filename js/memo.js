document.addEventListener("DOMContentLoaded", function() {
    loadMemoList();
});

async function fetchMemos() {
    try {
        const response = await fetch('/api/memo/');
        const memos = await response.json();
        displayMemos(memos);
    } catch (error) {
        console.error('Error fetching memos:', error);
    }
}

async function loadMemoList() {
    try {
        const response = await fetch('/api/memo/');
        if (!response.ok) {
            alert('Memo data를 불러오는 데 실패했습니다.');
            return;
        }
        const memos = await response.json();

        if (!memos || !Array.isArray(memos)) {
            alert('Memo 데이터가 유효하지 않습니다.');
            return;
        }
        const tableBody = document.getElementById('memoList').getElementsByTagName('tbody')[0];
        tableBody.innerHTML = '';

        memos.forEach(memo => {
            const row = tableBody.insertRow();

            const contentCell = row.insertCell();
            contentCell.className = 'memo-content-cell';
            contentCell.textContent = memo.content;

            const deleteCell = row.insertCell();
            deleteCell.className = 'delete-cell';

            const deleteButton = document.createElement('button');
            deleteButton.textContent = 'Delete';
            deleteButton.onclick = function() { deleteMemo(memo.id); };
            deleteButton.className = 'delete-button';
            deleteCell.appendChild(deleteButton);
        });
    } catch (error) {
        console.error('Error:', error);
        alert('Error loading memo list: ' + error.message);
    }
}

async function addMemo() {
    const content = document.getElementById('memoContent').value;
    const memo = { content };

    try {
        await fetch('/api/memo/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(memo),
        });
        loadMemoList();
    } catch (error) {
        console.error('Error adding memo:', error);
    }
}

async function deleteMemo(memoId) {
    try {
        const response = await fetch(`/api/memo/${memoId}`, {
            method: 'DELETE',
        });
        if (!response.ok) {
            throw new Error(`Error: ${response.status} ${response.statusText}`);
        }
        loadMemoList(); // 메모 삭제 후 메모 리스트를 다시 불러옵니다.
    } catch (error) {
        console.error('Error deleting memo:', error);
    }
}



