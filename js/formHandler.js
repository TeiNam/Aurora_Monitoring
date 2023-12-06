async function loadInstanceList() {
    try {
        const response = await fetch('/api/instance_setup/list_instances/');
        if (!response.ok) {
            throw new Error('Failed to fetch instance data');
        }
        const data = await response.json();
        const tableBody = document.getElementById('instanceTable').getElementsByTagName('tbody')[0];
        tableBody.innerHTML = '';

        data.instances.forEach(instance => {
            const row = tableBody.insertRow();
            row.insertCell().textContent = instance.instance_name;
            row.insertCell().textContent = instance.cluster_name;
            row.insertCell().textContent = instance.region;

            const deleteCell = row.insertCell();
            const deleteButton = document.createElement('button');
            deleteButton.textContent = 'Delete';
            deleteButton.onclick = function() { deleteInstance(instance.instance_name); };
            deleteCell.appendChild(deleteButton);
        });
    } catch (error) {
        console.error('Error:', error);
    }
}

async function deleteInstance(instanceName) {
    if (!confirm('정말 삭제하시겠습니까?')) return;

    try {
        const response = await fetch(`/api/instance_setup/delete_instance/?instance_name=${instanceName}`, {
            method: 'DELETE'
        });
        if (!response.ok) {
            throw new Error('Failed to delete instance');
        }
        const data = await response.json();
        alert(data.message);
        loadInstanceList();
    } catch (error) {
        console.error('Error:', error);
        alert('Error: ' + error.message);
    }
}

document.getElementById('rdsForm').onsubmit = async (e) => {
    e.preventDefault();

    if (!document.getElementById('instance_name').value.trim()) {
        alert('Instance Name is required.');
        return;
    }
    if (!document.getElementById('host').value.trim()) {
        alert('Host is required.');
        return;
    }
    if (!document.getElementById('user').value.trim()) {
        alert('User is required.');
        return;
    }
    if (!document.getElementById('password').value.trim()) {
        alert('Password is required.');
        return;
    }

    const formData = new FormData(e.target);
    const formProps = Object.fromEntries(formData);

    try {
        const response = await fetch('/api/instance_setup/add_instance/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formProps)
        });
        if(response.ok) {
            alert('RDS Instance added successfully!');
            loadInstanceList();
        } else {
            alert('Failed to add RDS Instance.');
        }
    } catch (error) {
        alert('Error: ' + error);
    }
};

window.onload = function() {
    loadInstanceList();
};
